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

type Messages struct {
	ID        int64  `json:"id" gorm:"column:id;type:bigint;primaryKey;autoIncrement;comment:id"`
	Biz       string `json:"biz" gorm:"column:biz;type:varchar(256);not null;comment:biz"`
	MessageID string `json:"message_id" gorm:"column:message_id;type:varchar(100);comment:message_id"`
	Topic     string `json:"topic" gorm:"column:topic;type:varchar(256);not null;comment:topic"`
	Content   string `json:"content" gorm:"column:content;type:text;not null;comment:content"`
	Status    int    `json:"status" gorm:"column:status;type:tinyint(1);not null;comment:status"`
	CreatedAt int64  `json:"created_at" gorm:"column:created_at;type:bigint;not null;comment:created_at"`
	UpdatedAt int64  `json:"updated_at" gorm:"column:updated_at;type:bigint;not null;comment:updated_at"`
}

type SendMessage struct {
	MessageID string
	Content   string
}
