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
#include "string_manipulation.h"

#include <cstddef>
#include <string_view>

namespace jogasaki::utils {

bool is_prefix(std::string_view target, std::string_view prefix) noexcept {
    if (target.size() < prefix.size()) {
        return false;
    }
    for(std::size_t i=0, n=prefix.size(); i < n; ++i) {
        if(prefix[i] != target[i]) {
            return false;
        }
    }
    return true;
}
}

