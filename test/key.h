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

#include <string>
#include "record.h"

namespace dc::data {

class key {
public:
    using key_type = record::key_type;
    using count_type = std::size_t;

    key(key_type key) : key_(key) {}

    key_type const& body() const noexcept {
        return key_;
    }

private:
    key_type key_;
};

static_assert(sizeof(key) == 8);
static_assert(std::alignment_of_v<key> == 8);


}
