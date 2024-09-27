/*
 * Copyright 2018-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "key_range.h"

#include <optional>

namespace jogasaki::dist {

key_range::key_type key_range::begin_key() const noexcept { return begin_key_; }

key_range::endpoint_type key_range::begin_endpoint() const noexcept { return begin_endpoint_; }

key_range::key_type key_range::end_key() const noexcept { return end_key_; }

key_range::endpoint_type key_range::end_endpoint() const noexcept { return end_endpoint_; }
} // namespace jogasaki::dist
