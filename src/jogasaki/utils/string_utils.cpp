/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "string_utils.h"
namespace jogasaki::utils {

[[nodiscard]] std::string_view rtrim(std::string_view sv) {
    while (!sv.empty() && sv.back() == ' ') {
        sv.remove_suffix(1);
    }
    return sv;
}

[[nodiscard]] std::string_view ltrim(std::string_view sv) {
    while (!sv.empty() && sv.front() == ' ') {
        sv.remove_prefix(1);
    }
    return sv;
}

} // namespace jogasaki::utils
