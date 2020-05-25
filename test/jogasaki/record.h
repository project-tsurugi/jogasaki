/*
 * Copyright 2018-2020 tsurugi project.
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
#pragma once

namespace jogasaki::data {

class record {
public:
    using key_type = std::int64_t;
    using value_type = double;

    record() = default;
    ~record() = default;
    record(record&& other) noexcept = default;
    record& operator=(record&& other) noexcept = default;
    record(key_type key, value_type value) : key_(key), value_(value) {}

    key_type const& key() const noexcept {
        return key_;
    }
    value_type const& value() const noexcept {
        return value_;
    }

private:
    key_type key_;
    value_type value_;
};

static_assert(sizeof(record) == 16);
static_assert(std::alignment_of_v<record> == 8);

}
