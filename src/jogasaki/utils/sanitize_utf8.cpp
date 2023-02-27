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
#include "sanitize_utf8.h"

#include <iomanip>
#include <sstream>

namespace jogasaki::utils {

std::string sanitize_utf8(std::string_view str) {
    //TODO use icu for complete utf8 handling - currently non printable ASCII characters are escaped
    std::stringstream out{};
    for(std::size_t idx = 0, n = str.size(); idx < n; ++idx) {
        auto c = *(str.data()+idx); //NOLINT
        if(std::isprint(c) != 0) {
            out << c;
        } else {
            out << std::string_view("\\x");
            out << std::hex << std::setw(2) << std::setfill('0')
                << static_cast<std::uint32_t>(*(str.data()+idx)); //NOLINT
        }
    }
    return out.str();
}

}

