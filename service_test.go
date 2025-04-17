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

//go:build e2e

package local_message_table

import (
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/TimeWtr/Bitly/pkg/logger"
	dl "github.com/TimeWtr/dis_lock"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	lg "gorm.io/gorm/logger"
)

func CalculateSharding(orderID any) (Dst, error) {
	totalSharding := 2 * 10
	shard := orderID.(int) % totalSharding

	dbIndex := shard / 10
	tableIndex := shard%10 + 1
	return Dst{
		Database: fmt.Sprintf("db_%d", dbIndex),
		Table:    fmt.Sprintf("message_%d", tableIndex),
	}, nil
}

func TestCalculateSharding(t *testing.T) {
	orderIDs := []int{1, 40, 20, 34, 16, 88, 92, 191, 103}
	for _, orderID := range orderIDs {
		dst, err := CalculateSharding(orderID)
		assert.Nil(t, err)
		t.Logf("id: %d dst: %v", orderID, dst)
	}
}

func TestNewMessageTable_Insert_And_Sync(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Username: "root",
		Password: "",
	})

	newLogger := lg.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		lg.Config{
			SlowThreshold:             time.Second, // Slow SQL threshold
			LogLevel:                  lg.Info,     // Log level
			IgnoreRecordNotFoundError: true,        // Ignore ErrRecordNotFound error for logger
			ParameterizedQueries:      false,       // Don't include params in the SQL log
			Colorful:                  true,        // Disable color
		},
	)

	dbs := make(map[string]DatabaseTable)
	dsn1 := "root:root@tcp(127.0.0.1:13306)/project?charset=utf8mb4&parseTime=True&loc=Local"
	db1, err := gorm.Open(mysql.Open(dsn1), &gorm.Config{
		Logger: newLogger,
	})
	assert.Nil(t, err)
	dbs["db_0"] = DatabaseTable{db: db1}

	dsn2 := "root:root@tcp(127.0.0.1:33061)/project?charset=utf8mb4&parseTime=True&loc=Local"
	db2, err := gorm.Open(mysql.Open(dsn2), &gorm.Config{
		Logger: newLogger,
	})
	assert.Nil(t, err)
	dbs["db_1"] = DatabaseTable{db: db2}

	// 创建10张消息表分表
	for key, item := range dbs {
		dt := item
		for i := 1; i <= 10; i++ {
			tableName := fmt.Sprintf("message_%d", i)
			if !dt.db.Migrator().HasTable(tableName) {
				err = dt.db.Table(tableName).AutoMigrate(&Messages{})
				assert.Nil(t, err)
			}
			dt.tables = append(dt.tables, tableName)
		}
		dbs[key] = dt
	}

	producer, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, nil)
	assert.Nil(t, err)
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zap.DebugLevel)
	zl, err := config.Build()
	assert.Nil(t, err)

	mp := NewMessageTable(dbs, CalculateSharding, producer,
		logger.NewZapLogger(zl),
		dl.NewClient(rdb))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = mp.AsyncWork(context.Background())
		if err != nil {
			t.Logf("async work err: %s", err.Error())
		}
	}()

	for i := 1; i <= 1000; i++ {
		err = mp.ExecTo(context.Background(), func(ctx context.Context, tx *gorm.DB) (Messages, error) {
			return Messages{
				Biz:     "test Biz",
				Topic:   "test_topic",
				Content: "test content",
			}, nil
		}, i+1)
		assert.Nil(t, err)
	}

	mp.Close()
	wg.Wait()
	t.Log("done!")
}

func TestNewMessageTable_Refresh_Failure_ContextDeadline(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Username: "root",
		Password: "",
	})

	dbs := make(map[string]DatabaseTable)
	dsn1 := "root:root@tcp(127.0.0.1:13306)/project?charset=utf8mb4&parseTime=True&loc=Local"
	db1, err := gorm.Open(mysql.Open(dsn1), &gorm.Config{})
	assert.Nil(t, err)
	dbs["db_0"] = DatabaseTable{db: db1}

	dsn2 := "root:root@tcp(127.0.0.1:33061)/project?charset=utf8mb4&parseTime=True&loc=Local"
	db2, err := gorm.Open(mysql.Open(dsn2), &gorm.Config{})
	assert.Nil(t, err)
	dbs["db_1"] = DatabaseTable{db: db2}

	// 创建10张消息表分表
	for key, item := range dbs {
		dt := item
		for i := 1; i <= 10; i++ {
			tableName := fmt.Sprintf("message_%d", i)
			if !dt.db.Migrator().HasTable(tableName) {
				err = dt.db.Table(tableName).AutoMigrate(&Messages{})
				assert.Nil(t, err)
			}
			dt.tables = append(dt.tables, tableName)
		}
		dbs[key] = dt
	}

	producer, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, nil)
	assert.Nil(t, err)

	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zap.InfoLevel)
	zl, err := config.Build()
	assert.Nil(t, err)

	mp := NewMessageTable(dbs, CalculateSharding, producer,
		logger.NewZapLogger(zl),
		dl.NewClient(rdb))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = mp.AsyncWork(ctx)
		assert.Equal(t, context.DeadlineExceeded, err)
	}()
	for i := 1; i <= 2000; i++ {
		err = mp.ExecTo(context.Background(), func(ctx context.Context, tx *gorm.DB) (Messages, error) {
			return Messages{
				Biz:     "test Biz",
				Topic:   "test_topic",
				Content: "test content",
			}, nil
		}, i+1)
		assert.Nil(t, err)
	}

	mp.Close()
	wg.Wait()
	t.Log("done!")
}

func TestNewMessageTable_MiddleWare_Error(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Username: "root",
		Password: "",
	})

	newLogger := lg.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		lg.Config{
			SlowThreshold:             time.Second, // Slow SQL threshold
			LogLevel:                  lg.Silent,   // Log level
			IgnoreRecordNotFoundError: true,        // Ignore ErrRecordNotFound error for logger
			ParameterizedQueries:      false,       // Don't include params in the SQL log
			Colorful:                  true,        // Disable color
		},
	)

	dbs := make(map[string]DatabaseTable)
	dsn1 := "root:root@tcp(127.0.0.1:13306)/project?charset=utf8mb4&parseTime=True&loc=Local"
	db1, err := gorm.Open(mysql.Open(dsn1), &gorm.Config{
		Logger: newLogger,
	})
	assert.Nil(t, err)
	dbs["db_0"] = DatabaseTable{db: db1}

	dsn2 := "root:root@tcp(127.0.0.1:33061)/project?charset=utf8mb4&parseTime=True&loc=Local"
	db2, err := gorm.Open(mysql.Open(dsn2), &gorm.Config{
		Logger: newLogger,
	})
	assert.Nil(t, err)
	dbs["db_1"] = DatabaseTable{db: db2}

	// 创建10张消息表分表
	for key, item := range dbs {
		dt := item
		for i := 1; i <= 10; i++ {
			tableName := fmt.Sprintf("message_%d", i)
			if !dt.db.Migrator().HasTable(tableName) {
				err = dt.db.Table(tableName).AutoMigrate(&Messages{})
				assert.Nil(t, err)
			}
			dt.tables = append(dt.tables, tableName)
		}
		dbs[key] = dt
	}

	producer, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, nil)
	assert.Nil(t, err)
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zap.ErrorLevel)
	zl, err := config.Build()
	assert.Nil(t, err)

	mp := NewMessageTable(dbs, CalculateSharding, producer,
		logger.NewZapLogger(zl),
		dl.NewClient(rdb))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = mp.AsyncWork(context.Background())
		if err != nil {
			t.Logf("async work err: %s", err.Error())
		}
	}()
	for i := 1; i <= 10; i++ {
		err = mp.ExecTo(context.Background(), func(ctx context.Context, tx *gorm.DB) (Messages, error) {
			return Messages{
				Biz:     "test Biz",
				Topic:   "test_topic",
				Content: "test content",
			}, nil
		}, i+1)
		time.Sleep(time.Second)
		assert.Nil(t, err)
	}

	mp.Close()
	wg.Wait()
	t.Log("done!")
}

func BenchmarkNewMessageTable_Insert_And_Sync(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Username: "root",
		Password: "",
	})

	newLogger := lg.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		lg.Config{
			SlowThreshold:             time.Second, // Slow SQL threshold
			LogLevel:                  lg.Info,     // Log level
			IgnoreRecordNotFoundError: true,        // Ignore ErrRecordNotFound error for logger
			ParameterizedQueries:      false,       // Don't include params in the SQL log
			Colorful:                  false,       // Disable color
		},
	)

	dbs := make(map[string]DatabaseTable)
	dsn1 := "root:root@tcp(127.0.0.1:13306)/project?charset=utf8mb4&parseTime=True&loc=Local"
	db1, err := gorm.Open(mysql.Open(dsn1), &gorm.Config{
		Logger: newLogger,
	})
	assert.Nil(b, err)
	dbs["db_0"] = DatabaseTable{db: db1}

	dsn2 := "root:root@tcp(127.0.0.1:33061)/project?charset=utf8mb4&parseTime=True&loc=Local"
	db2, err := gorm.Open(mysql.Open(dsn2), &gorm.Config{
		Logger: newLogger,
	})
	assert.Nil(b, err)
	dbs["db_1"] = DatabaseTable{db: db2}

	producer, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, nil)
	assert.Nil(b, err)
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zap.ErrorLevel)
	zl, err := config.Build()
	assert.Nil(b, err)

	mp := NewMessageTable(dbs, CalculateSharding, producer,
		logger.NewZapLogger(zl),
		dl.NewClient(rdb))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = mp.AsyncWork(context.Background())
		if err != nil {
			b.Logf("async work err: %s", err.Error())
		}
	}()

	for i := 1; i <= b.N; i++ {
		err = mp.ExecTo(context.Background(), func(ctx context.Context, tx *gorm.DB) (Messages, error) {
			return Messages{
				Biz:     "test Biz",
				Topic:   "test_topic",
				Content: "test content",
			}, nil
		}, i+1)
		assert.Nil(b, err)
	}

	mp.Close()
	wg.Wait()
	b.Log("done!")
}
