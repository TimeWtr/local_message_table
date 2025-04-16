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

import "errors"

var (
	ErrTopicEmpty   = errors.New("topic is empty")
	ErrContentEmpty = errors.New("content is empty")
)

var (
	ErrOverLimit = errors.New("middleware error and over max rate")
)
