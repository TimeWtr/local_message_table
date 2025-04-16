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
	"container/list"
	"sync"
	"time"

	"golang.org/x/net/context"
)

type Limiter interface {
	// Allow 是否允许继续执行
	Allow(ctx context.Context) (bool, error)
}

// SlidingWindow 基于内存的滑动窗口算法
type SlidingWindow struct {
	// 滑动窗口的速率
	rate int64
	// 滑动窗口的大小
	interval time.Duration
	// 窗口内的请求队列
	l *list.List
	// 加锁保护
	mu sync.Mutex
}

func NewSlidingWindow(rate int64, interval time.Duration) Limiter {
	return &SlidingWindow{
		rate:     rate,
		interval: interval,
		l:        list.New(),
		mu:       sync.Mutex{},
	}
}

func (s *SlidingWindow) Allow(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	now := time.Now().UnixNano()
	// 快路径
	s.mu.Lock()
	if s.l.Len() < int(s.rate) {
		s.l.PushBack(now)
		s.mu.Unlock()
		return true, nil
	}
	s.mu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.l.Front()
	startTime := now - s.interval.Nanoseconds()
	for e != nil && e.Value.(int64) <= startTime {
		s.l.Remove(e)
		e = e.Next()
	}

	if s.l.Len() < int(s.rate) {
		s.l.PushBack(now)
		return true, nil
	}

	return false, ErrOverLimit
}
