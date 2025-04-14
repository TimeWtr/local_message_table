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

import "time"

const (
	DefaultInterval = 30 * time.Second
	DefaultLimit    = 100
)

type MessageStatus int

const (
	// MessageStatusNotSend 未发送状态
	MessageStatusNotSend MessageStatus = iota
	// MessageStatusSendSuccess 发送成功
	MessageStatusSendSuccess
	// MessageStatusSendFailure 发送失败
	MessageStatusSendFailure
)

func (s MessageStatus) String() string {
	switch s {
	case MessageStatusNotSend:
		return "message not send"
	case MessageStatusSendSuccess:
		return "message send success"
	case MessageStatusSendFailure:
		return "failed to send message"
	default:
		return "unknown status"
	}
}
