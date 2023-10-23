/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include "request_statistics.h"

namespace jogasaki {

void request_execution_counter::count(std::int64_t arg) {
    count_ += arg;
}

std::int64_t request_execution_counter::count() const noexcept {
    return count_;
}

request_execution_counter& request_statistics::counter(counter_kind kind) {
    return entity_[static_cast<std::underlying_type_t<counter_kind>>(kind)];
}
}

