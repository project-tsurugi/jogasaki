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
#include "sanitize_utf8.h"

#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <sstream>

namespace jogasaki::utils {

namespace {

/**
 * @brief Check if a byte is a valid UTF-8 continuation byte (10xxxxxx).
 * @param b Byte to check
 * @return true if the byte is a continuation byte, false otherwise
 */
inline bool is_continuation_byte(std::uint8_t b) {
    return (b & 0xC0U) == 0x80U;
}

/**
 * @brief Validate and decode a UTF-8 sequence starting at the given position.
 * @param data Pointer to the start of the string data
 * @param idx Current position in the string
 * @param n Total size of the string
 * @param[out] seq_len Length of the UTF-8 sequence if valid
 * @param[out] code_point Decoded Unicode code point if valid
 * @return true if the sequence is valid UTF-8, false otherwise
 */
bool validate_utf8_sequence(  //NOLINT(readability-function-cognitive-complexity)
    std::string_view str,
    std::size_t idx,
    std::size_t& seq_len,
    std::uint32_t& code_point
) {
    std::size_t n = str.size();
    auto byte0 = static_cast<std::uint8_t>(str[idx]);

    // ASCII (0xxxxxxx)
    if (byte0 <= 0x7FU) {
        seq_len = 1;
        code_point = byte0;
        return true;
    }

    // 2-byte sequence (110xxxxx 10xxxxxx)
    if ((byte0 & 0xE0U) == 0xC0U) {
        if (idx + 1 >= n) {
            return false;
        }
        auto byte1 = static_cast<std::uint8_t>(str[idx + 1]);
        if (! is_continuation_byte(byte1)) {
            return false;
        }
        code_point = ((byte0 & 0x1FU) << 6U) | (byte1 & 0x3FU);
        // Check for overlong encoding
        if (code_point < 0x80U) {
            return false;
        }
        seq_len = 2;
        return true;
    }

    // 3-byte sequence (1110xxxx 10xxxxxx 10xxxxxx)
    if ((byte0 & 0xF0U) == 0xE0U) {
        if (idx + 2 >= n) {
            return false;
        }
        auto byte1 = static_cast<std::uint8_t>(str[idx + 1]);
        auto byte2 = static_cast<std::uint8_t>(str[idx + 2]);
        if (! is_continuation_byte(byte1) || ! is_continuation_byte(byte2)) {
            return false;
        }
        code_point = ((byte0 & 0x0FU) << 12U) | ((byte1 & 0x3FU) << 6U) | (byte2 & 0x3FU);
        // Check for overlong encoding
        if (code_point < 0x800U) {
            return false;
        }
        // Check for UTF-16 surrogates (U+D800 to U+DFFF)
        if (code_point >= 0xD800U && code_point <= 0xDFFFU) {
            return false;
        }
        seq_len = 3;
        return true;
    }

    // 4-byte sequence (11110xxx 10xxxxxx 10xxxxxx 10xxxxxx)
    if ((byte0 & 0xF8U) == 0xF0U) {
        if (idx + 3 >= n) {
            return false;
        }
        auto byte1 = static_cast<std::uint8_t>(str[idx + 1]);
        auto byte2 = static_cast<std::uint8_t>(str[idx + 2]);
        auto byte3 = static_cast<std::uint8_t>(str[idx + 3]);
        if (! is_continuation_byte(byte1) || ! is_continuation_byte(byte2) || ! is_continuation_byte(byte3)) {
            return false;
        }
        code_point = ((byte0 & 0x07U) << 18U) | ((byte1 & 0x3FU) << 12U) | ((byte2 & 0x3FU) << 6U) | (byte3 & 0x3FU);
        // Check for overlong encoding and maximum valid Unicode code point
        if (code_point < 0x10000U || code_point > 0x10FFFFU) {
            return false;
        }
        seq_len = 4;
        return true;
    }

    // Invalid UTF-8 start byte
    return false;
}

/**
 * @brief Check if a Unicode code point is a printable character.
 * @param code_point Unicode code point
 * @return true if the code point is printable, false otherwise
 */
bool is_printable_unicode(std::uint32_t code_point) {
    // ASCII printable range (0x20-0x7E, excluding control characters)
    if (code_point >= 0x20 && code_point <= 0x7E) {
        return true;
    }
    // Extended Unicode printable range (excluding control characters)
    // C0 controls: 0x00-0x1F
    // DEL: 0x7F
    // C1 controls: 0x80-0x9F
    if (code_point < 0x20 || code_point == 0x7F || (code_point >= 0x80 && code_point <= 0x9F)) {
        return false;
    }
    // All other valid Unicode code points are considered printable
    return true;
}

/**
 * @brief Escape a single byte in \x{HH} format.
 * @param out Output stream
 * @param b Byte to escape
 */
void escape_byte(std::stringstream& out, std::uint8_t b) {
    out << std::string_view("\\x{");
    out << std::hex << std::setw(2) << std::setfill('0') << static_cast<std::uint32_t>(b);
    out << std::string_view("}");
}

} // namespace

std::string sanitize_utf8(std::string_view str) {
    std::stringstream out{};
    std::size_t n = str.size();

    for (std::size_t idx = 0; idx < n; ) {
        std::size_t seq_len = 0;
        std::uint32_t code_point = 0;

        if (validate_utf8_sequence(str, idx, seq_len, code_point)) {
            // Valid UTF-8 sequence
            if (is_printable_unicode(code_point)) {
                // Output the valid UTF-8 sequence as-is
                for (std::size_t i = 0; i < seq_len; ++i) {
                    out << str[idx + i];
                }
            } else {
                // Escape each byte of the non-printable sequence
                for (std::size_t i = 0; i < seq_len; ++i) {
                    escape_byte(out, static_cast<std::uint8_t>(str[idx + i]));
                }
            }
            idx += seq_len;
        } else {
            // Invalid UTF-8 byte - escape it
            escape_byte(out, static_cast<std::uint8_t>(str[idx]));
            ++idx;
        }
    }

    return out.str();
}

}

