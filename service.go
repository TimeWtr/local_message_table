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
	"sync"
	"time"

	"github.com/IBM/sarama"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"

	"github.com/TimeWtr/Bitly/pkg/logger"
)

var (
	once         sync.Once
	messageTable *MessageTable
)

// BizFn 执行业务的方法
type BizFn func(ctx context.Context, tx *gorm.DB) (Messages, error)

// ShardingFn 执行分片的方法
type ShardingFn func(ctx context.Context, key any) (Dst, error)

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
}

func NewMessageTable(dbs map[string]DatabaseTable,
	fn ShardingFn,
	producer sarama.SyncProducer,
	l logger.Logger,
	opts ...Options) MessagePusher {
	once.Do(func() {
		messageTable = &MessageTable{
			dbs:      dbs,
			Sharding: fn,
			producer: producer,
			closeCh:  make(chan struct{}),
			once:     sync.Once{},
			interval: DefaultInterval,
			limit:    DefaultLimit,
			l:        l,
		}
		for _, opt := range opts {
			opt(messageTable)
		}
	})

	return messageTable
}

func (m *MessageTable) AsyncWork(ctx context.Context) error {
	// 查询未发送的消息
	var eg errgroup.Group
	for _, d := range m.dbs {
		dt := d
		for _, table := range dt.tables {
			eg.Go(func() error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-m.closeCh:
						return nil
					default:
					}

					ctxl, cancel := context.WithTimeout(ctx, time.Second)
					now := time.Now().UnixMilli() - m.interval.Milliseconds()
					var msgs []Messages
					err := dt.db.WithContext(ctx).Model(&Messages{}).Table(table).
						Where("status = ? AND updated_at < ?", MessageStatusNotSend, now).
						Offset(0).
						Limit(m.limit).
						Find(&msgs).Error
					cancel()
					if err != nil {
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

					// 批量更新消息状态，这个地方可能会出现更新失败但推送成功的情况，还会被捞出来发送
					// 消息消费方需要实现消息幂等
					ctxl, cancel = context.WithTimeout(ctx, time.Second)
					err = dt.db.WithContext(ctxl).Model(&Messages{}).
						Table(table).
						Where("id IN (?)", batchIDS).
						Updates(map[string]interface{}{
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
	return eg.Wait()
}

func (m *MessageTable) ExecTo(ctx context.Context, fn BizFn, shardingKey any) error {
	// 通过分片方法来获取数据库和表信息
	dst, err := m.Sharding(ctx, shardingKey)
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
	return dt.db.WithContext(ctx).Model(&Messages{}).
		Where("id = ?", msg.ID).
		Updates(map[string]interface{}{
			"status":     MessageStatusSendSuccess,
			"updated_at": time.Now().UnixMilli(),
		}).Error
}

func (m *MessageTable) sendMessage(msg Messages) error {
	_, _, err := m.producer.SendMessage(&sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.StringEncoder(msg.Content),
	})

	return err
}

func (m *MessageTable) Close() {
	once.Do(func() {
		close(m.closeCh)
	})
}

type Dst struct {
	Database string
	Table    string
}
