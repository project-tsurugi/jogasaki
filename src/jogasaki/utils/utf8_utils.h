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
#pragma once

#include <string_view>
#include <cstddef>

namespace jogasaki::utils {

/**
 * @brief Checks whether the given byte is a UTF-8 continuation byte.
 *
 * Continuation bytes in UTF-8 are those that start with the bits `10xxxxxx`.
 * This function checks if the given character matches this pattern.
 *
 * @see https://datatracker.ietf.org/doc/html/rfc3629#section-3
 *
 * @param c The character to check.
 * @return true if it is a continuation byte, false otherwise.
 */
[[nodiscard]] constexpr bool is_continuation_byte(unsigned char c) noexcept {
    return (c & 0xC0U) == 0x80U;
}

enum class encoding_type { ASCII_1BYTE, UTF8_2BYTE, UTF8_3BYTE, UTF8_4BYTE, INVALID };
/**
 * @brief Detects the UTF-8 encoding type of the character at the specified offset.
 *
 * @param view The UTF-8 encoded string view.
 * @param offset The position to inspect.
 * @return encoding_type Type of UTF-8 encoding
 */
[[nodiscard]] encoding_type detect_next_encoding(std::string_view view, std::size_t offset);
/*
 * @param e encoding_type
 * @return byte size of the encoding type
 */
[[nodiscard]] constexpr std::size_t get_byte(encoding_type e) noexcept {
    switch (e) {
        case encoding_type::ASCII_1BYTE: return 1;
        case encoding_type::UTF8_2BYTE: return 2;
        case encoding_type::UTF8_3BYTE: return 3;
        case encoding_type::UTF8_4BYTE: return 4;
        case encoding_type::INVALID: return 0;
    }
    return 0;
}
/**
 * @brief Checks whether the given string view is a valid UTF-8 encoded sequence.
 *
 * The function iterates through the bytes of the input string and verifies
 * that each character conforms to the UTF-8 encoding rules.
 *
 * @param view A string view representing the byte sequence to validate.
 * @return true if the sequence is valid UTF-8; false otherwise.
 */
[[nodiscard]] bool is_valid_utf8(std::string_view view) noexcept;

} // namespace jogasaki::utils
