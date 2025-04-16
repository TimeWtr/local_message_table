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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestNewSlidingWindow(t *testing.T) {
	sw := NewSlidingWindow(3, 5*time.Second)
	for i := 0; i < 10; i++ {
		t.Logf("index: %d\n", i)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		allow, err := sw.Allow(ctx)
		cancel()
		if i < 3 {
			assert.NoError(t, err)
			assert.True(t, allow)
		} else {
			assert.Equal(t, ErrOverLimit, err)
			assert.False(t, allow)
		}
	}
}

func TestNewSlidingWindow_ReAllow(t *testing.T) {
	sw := NewSlidingWindow(3, 4*time.Second)
	for i := 0; i < 8; i++ {
		t.Logf("index: %d\n", i)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		allow, err := sw.Allow(ctx)
		cancel()
		switch i {
		case 0, 1, 2:
			assert.NoError(t, err)
			assert.True(t, allow)
		case 3, 4, 5:
			assert.Equal(t, ErrOverLimit, err)
			assert.False(t, allow)
			time.Sleep(1500 * time.Millisecond)
		default:
			assert.NoError(t, err)
			assert.True(t, allow)
		}
	}
}
