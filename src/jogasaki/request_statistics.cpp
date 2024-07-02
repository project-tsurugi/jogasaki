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
#include <jogasaki/request_statistics.h>

namespace jogasaki {

void request_execution_counter::count(std::int64_t arg) {
    if(! count_.has_value()) {
        count_ = 0;
    }
    count_ = *count_ + arg;
}

std::optional<std::int64_t> request_execution_counter::count() const noexcept {
    return count_;
}

bool request_execution_counter::has_value() const noexcept {
    return count_.has_value();
}

request_execution_counter& request_statistics::counter(counter_kind kind) {
    return entity_[static_cast<std::underlying_type_t<counter_kind>>(kind)];
}

void request_statistics::each_counter(
    request_statistics::each_counter_consumer consumer  //NOLINT(performance-unnecessary-value-param)
) const noexcept {
    for(auto&& [k, e] : entity_) {
        if(! e.count().has_value()) continue;
        consumer(static_cast<counter_kind>(k), e);
    }
}

void request_statistics::start_time(request_statistics::clock::time_point arg) noexcept {
    start_time_ = arg;
}
void request_statistics::end_time(request_statistics::clock::time_point arg) noexcept {
    end_time_ = arg;
}

}  // namespace jogasaki
