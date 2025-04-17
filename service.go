// Copyright 2025 TimeWtr
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local_message_table

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/IBM/sarama"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"

	"github.com/TimeWtr/Bitly/pkg/logger"
	dl "github.com/TimeWtr/dis_lock"
)

var (
	once         sync.Once
	messageTable *MessageTable
)

// BizFn 执行业务的方法
type BizFn func(ctx context.Context, tx *gorm.DB) (Messages, error)

// ShardingFn 执行分片的方法
type ShardingFn func(key any) (Dst, error)

type Options func(*MessageTable)

// WithInterval 设置异步补偿任务从消息表捞取消息的时间间隔，如果不设置默认为30秒
func WithInterval(interval time.Duration) Options {
	return func(m *MessageTable) {
		m.interval = interval
	}
}

// WithLimit 设置异步补偿任务批量消息的条数，默认为100条
func WithLimit(limit int) Options {
	return func(m *MessageTable) {
		m.limit = limit
	}
}

// MessagePusher 本地消息表的通用接口
type MessagePusher interface {
	// AsyncWork 异步任务用于同步消息推送失败后的异步补偿机制，调用方需要开启异步调用
	AsyncWork(ctx context.Context) error
	// ExecTo 用于执行业务、写入消息表、同步推送事务消息
	ExecTo(ctx context.Context, fn BizFn, shardingKey any) error
	// Close 关闭本地消息表
	Close()
}

type DatabaseTable struct {
	// 数据库链接
	db *gorm.DB
	// 当前数据库下的所有消息分表
	tables []string
}

type MessageTable struct {
	// 数据库链接池
	dbs map[string]DatabaseTable
	// 获取分片的方法
	Sharding ShardingFn
	// 消息队列同步发送
	producer sarama.SyncProducer
	// 关闭消息表
	closeCh chan struct{}
	// 单例
	once sync.Once
	// 未发送消息的捞取时间间隔
	interval time.Duration
	// 异步补偿机制每次批量查询未发送数据的条数
	limit int
	// 日志适配器
	l logger.Logger
	// 分布式锁控制只有一个实例可以真正运行后台的异步补偿任务
	lock *dl.Client
	// 限流器，控制中间件错误的频率，当指定窗口周期内超过一定频率的中间件错误(MySQL、Kafka)，
	// 就主动停止异步补偿机制，释放所有goroutine，返回错误信息并释放分布式锁
	errLimit Limiter
	cancel   context.CancelFunc
	// UUID 生成器
	u uuid.UUID
}

func NewMessageTable(dbs map[string]DatabaseTable,
	fn ShardingFn,
	producer sarama.SyncProducer,
	l logger.Logger,
	lock *dl.Client,
	opts ...Options) MessagePusher {
	once.Do(func() {
		u, err := uuid.NewUUID()
		if err != nil {
			panic(err)
		}

		messageTable = &MessageTable{
			dbs:      dbs,
			Sharding: fn,
			producer: producer,
			closeCh:  make(chan struct{}),
			once:     sync.Once{},
			lock:     lock,
			interval: DefaultInterval,
			limit:    DefaultLimit,
			l:        l,
			u:        u,
		}

		for _, opt := range opts {
			opt(messageTable)
		}
	})

	return messageTable
}

func (m *MessageTable) AsyncWork(ctx context.Context) error {
	ctx, m.cancel = context.WithCancel(ctx)
	// 先尝试获取分布式锁
	lock, err := m.lock.TryLock(ctx, LockKey, LockID, time.Minute)
	if err != nil {
		m.l.Errorf("尝试获取分布式锁失败",
			logger.Field{Key: "error", Val: err.Error()})
		return err
	}

	m.l.Infof("获取分布式锁成功")

	errCh := make(chan error)
	defer close(errCh)

	refreshCloseCh := make(chan struct{})

	// 异步定时续约分布式锁
	go func() {
		defer func() {
			close(refreshCloseCh)
		}()

		er := lock.AutoRefresh(context.Background(), 5*time.Second, 15*time.Second, time.Second)
		if er != nil {
			errCh <- er
		}

		m.l.Infof("结束续约协程")
	}()

	// 异步监控是否需要主动释放分布式锁
	go func() {
		defer func() {
			_ = lock.UnLock(ctx)
		}()

		for {
			select {
			case <-ctx.Done():
				m.l.Debugf("超时取消")
				return
			case <-m.closeCh:
				m.l.Debugf("收到退出信号")
				return
			case <-refreshCloseCh:
				m.l.Debugf("收到续约关闭信号")
				return
			}
		}
	}()

	errLimit := NewSlidingWindow(5, 5*time.Second)

	// 批量查询未发送的消息，执行异步补偿任务
	var eg errgroup.Group
	for _, d := range m.dbs {
		dt := d
		for _, table := range dt.tables {
			tb := table
			eg.Go(func() error {
				for {
					select {
					case <-ctx.Done():
						m.l.Debugf("超时取消", logger.Field{Key: "table", Val: tb})
						return ctx.Err()
					case <-m.closeCh:
						m.l.Debugf("收到停止信号，退出补偿协程",
							logger.Field{Key: "table", Val: tb})
						return nil
					case <-refreshCloseCh:
						// 分布式锁异步刷新结束，锁已释放
						m.l.Debugf("收到续约关闭信号")
						return nil
					default:
					}

					ctxl, cancel := context.WithTimeout(ctx, time.Second)
					now := time.Now().UnixMilli() - m.interval.Milliseconds()
					var msgs []Messages
					err = dt.db.WithContext(ctx).Model(&Messages{}).Table(tb).
						Where("status = ? AND updated_at < ?", MessageStatusNotSend, now).
						Offset(0).Limit(m.limit).Find(&msgs).Error
					if err != nil {
						if m.isMiddlewareError(err) {
							ok, _ := errLimit.Allow(ctx)
							if !ok {
								m.l.Errorf("MySQL链接异常，停止异步补偿任务")
								m.cancel()
								return ErrOverLimit
							}
						}
						m.l.Errorf("批量查询未发送消息失败", logger.Field{Key: "error", Val: err.Error()})
						continue
					}

					if len(msgs) == 0 {
						continue
					}

					var batchIDS []int64
					for _, msg := range msgs {
						// 同步推送事务消息到消息队列，调用方不关心推送状态
						err = m.sendMessage(msg)
						if err != nil {
							if m.isMiddlewareError(err) {
								ok, _ := errLimit.Allow(ctx)
								if !ok {
									m.l.Errorf("Kafka链接异常，停止异步补偿任务")
									m.cancel()
									return ErrOverLimit
								}
							}
							m.l.Errorf("异步补偿任务推送失败",
								logger.Field{Key: "id", Val: msg.ID},
								logger.Field{Key: "error", Val: err.Error()})
							continue
						}

						batchIDS = append(batchIDS, msg.ID)
					}
					if len(batchIDS) == 0 {
						continue
					}

					m.l.Debugf("异步补偿推送消息成功，消息id为：%s", logger.Field{
						Key: "IDS",
						Val: batchIDS,
					})

					// 批量更新消息状态，这个地方可能会出现更新失败但推送成功的情况，还会被捞出来发送
					// 消息消费方需要实现消息幂等
					ctxl, cancel = context.WithTimeout(ctx, time.Second)
					err = dt.db.Debug().WithContext(ctxl).Table(tb).
						Where("id IN (?)", batchIDS).Updates(map[string]interface{}{
						"status":     MessageStatusSendSuccess,
						"updated_at": time.Now().UnixMilli(),
					}).Error
					cancel()
					if err != nil {
						m.l.Errorf("异步补偿批量更新状态失败", logger.Field{Key: "error", Val: err.Error()})
					}
				}
			})
		}
	}
	if err = eg.Wait(); err != nil {
		return err
	}

	m.l.Infof("结束goroutine")

	select {
	case err = <-errCh:
		return err
	default:
		return nil
	}
}

func (m *MessageTable) ExecTo(ctx context.Context, fn BizFn, shardingKey any) error {
	// 通过分片方法来获取数据库和表信息
	dst, err := m.Sharding(shardingKey)
	if err != nil {
		m.l.Errorf("获取分片信息失败",
			logger.Field{Key: "Sharding Key", Val: shardingKey},
			logger.Field{Key: "error", Val: err.Error()},
		)
		return err
	}

	dt := m.dbs[dst.Database]
	var msg Messages
	// 开启事务
	err = dt.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 执行业务程序
		msg, err = fn(ctx, tx)
		if err != nil {
			return err
		}

		if msg.Topic == "" {
			return ErrTopicEmpty
		}

		if msg.Content == "" {
			return ErrContentEmpty
		}

		now := time.Now().UnixMilli()
		msg.CreatedAt = now
		msg.UpdatedAt = now
		if msg.MessageID == "" {
			msg.MessageID = m.u.String()
		}

		return tx.Model(&Messages{}).Table(dst.Table).Create(&msg).Error
	})
	if err != nil {
		m.l.Errorf("执行失败",
			logger.Field{Key: "Sharding Key", Val: shardingKey},
			logger.Field{Key: "database", Val: dst.Database},
			logger.Field{Key: "error", Val: err.Error()})
		return err
	}

	// 同步推送事务消息到消息队列，调用方不关心推送状态
	if err = m.sendMessage(msg); err != nil {
		m.l.Errorf("同步推送失败",
			logger.Field{Key: "id", Val: msg.ID},
			logger.Field{Key: "error", Val: err.Error()})
		return nil
	}

	// 更新消息推送状态
	return dt.db.WithContext(ctx).Table(dst.Table).
		Where("id = ?", msg.ID).
		Updates(map[string]interface{}{
			"status":     MessageStatusSendSuccess,
			"updated_at": time.Now().UnixMilli(),
		}).Error
}

func (m *MessageTable) sendMessage(msg Messages) error {
	val := SendMessage{
		MessageID: msg.MessageID,
		Content:   msg.Content,
	}

	bs, err := json.Marshal(val)
	if err != nil {
		return err
	}

	_, _, err = m.producer.SendMessage(&sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.ByteEncoder(bs),
	})

	return err
}

func (m *MessageTable) isMiddlewareError(err error) bool {
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound), errors.Is(err, context.DeadlineExceeded):
		return false
	case errors.Is(err, sarama.ErrMessageTooLarge):
		return false
	default:
		return true
	}
}

func (m *MessageTable) Close() {

	m.once.Do(func() {
		close(m.closeCh)
	})
}

type Dst struct {
	Database string
	Table    string
}
