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
#include "hex_to_octet.h"

#include <cstdint>
#include <string>
#include <string_view>

namespace jogasaki::utils {

constexpr std::uint32_t invalid_hex_digit = static_cast<std::uint32_t>(-1);

static std::uint32_t hex_digit_to_uint(char c) {
    if(c >= '0' && c <= '9') {
        return c - '0';
    }
    if(c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    if(c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    return invalid_hex_digit;
}

bool hex_to_octet(std::string_view src, std::string& out) noexcept {
    if(src.size() % 2 != 0) {
        return false;
    }
    out.resize(src.size()/2);
    for(std::size_t i=0, n=src.size()/2; i < n;++i) {
        auto h = hex_digit_to_uint(src.at(2*i));
        auto l = hex_digit_to_uint(src.at(2*i+1));
        if(h == invalid_hex_digit || l == invalid_hex_digit) {
            return false;
        }
        out.at(i) = static_cast<char>((h << 4U) | l);
    }
    return true;
}

}  // namespace jogasaki::utils
