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
#include "convert_control_characters.h"

#include <cstddef>
#include <sstream>

namespace jogasaki::utils {

std::string convert_control_characters(std::string_view str) {
    constexpr char replacement_char = '!';
    std::stringstream out{};
    for(std::size_t idx = 0, n = str.size(); idx < n; ++idx) {
        auto c = *(str.data()+idx); //NOLINT
        if(c < 0) {
            // msb is set - discard in order to avoid garbage in UTF-8 chars
        } else if(c <= 0x1f) {
            out << replacement_char;
        } else {
            out << c;
        }
    }
    return out.str();
}

}
