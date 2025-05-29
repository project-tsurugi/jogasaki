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
#include "utf8_utils.h"

namespace jogasaki::utils {

constexpr bool is_continuation_byte(unsigned char c) noexcept { return (c & 0xC0U) == 0x80U; }
encoding_type detect_next_encoding(std::string_view view, const size_t offset) {
    const auto size = view.size();
    if (offset >= size) return encoding_type::INVALID;
    const auto offset_2nd = offset + 1;
    const auto offset_3rd = offset + 2;
    const auto first      = static_cast<unsigned char>(view[offset]);
    if (first <= 0x7FU) { return encoding_type::ASCII_1BYTE; }
    if (first >= 0xC2U && first <= 0xDFU) {
        return (size >= 2 && is_continuation_byte(view[offset_2nd])) ? encoding_type::UTF8_2BYTE
                                                                     : encoding_type::INVALID;
    }
    if (first >= 0xE0U && first <= 0xEFU) {
        return (size >= 3 && is_continuation_byte(view[offset_2nd]) &&
                   is_continuation_byte(view[offset_3rd]))
                   ? encoding_type::UTF8_3BYTE
                   : encoding_type::INVALID;
    }
    if (first >= 0xF0U && first <= 0xF4U) {
        const auto offset_4th = offset + 3;
        return (size >= 4 && is_continuation_byte(view[offset_2nd]) &&
                   is_continuation_byte(view[offset_3rd]) && is_continuation_byte(view[offset_4th]))
                   ? encoding_type::UTF8_4BYTE
                   : encoding_type::INVALID;
    }
    return encoding_type::INVALID;
}
std::size_t get_byte(encoding_type e) noexcept {
    switch (e) {
        case encoding_type::ASCII_1BYTE: return 1;
        case encoding_type::UTF8_2BYTE: return 2;
        case encoding_type::UTF8_3BYTE: return 3;
        case encoding_type::UTF8_4BYTE: return 4;
        case encoding_type::INVALID: return 0;
    }
    return 0;
}

} // namespace jogasaki::utils
