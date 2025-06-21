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

#include <string>
#include <string_view>

namespace jogasaki::utils {

/**
 * @brief Encodes binary data into a Base64-encoded string.
 *
 * This function takes an input binary string (as a string view)
 * and returns its Base64-encoded representation.
 *
 * @param input The input data to encode, given as a std::string_view.
 *              It may contain any binary content.
 * @return A Base64-encoded std::string representing the input data.
 *         The result will only contain printable ASCII characters
 *         (`A–Z`, `a–z`, `0–9`, `+`, `/`, and `=` for padding).
 *
 * @note The returned string always has a length that is a multiple of 4.
 *
 * @see https://datatracker.ietf.org/doc/html/rfc4648 for Base64 encoding specification.
 */
[[nodiscard]] std::string encode_base64(std::string_view input);
/**
 * @brief Checks whether the given string is a valid Base64-encoded string.
 *
 * This function verifies if the input string:
 * - has a length that is a multiple of 4,
 * - contains only valid Base64 characters (A–Z, a–z, 0–9, '+', '/', and '='),
 * - and has correct padding (at most two '=' characters, only at the end).
 *
 * @param sv the input string to check, passed as a std::string_view.
 * @return true if the string is a syntactically valid Base64-encoded string,
 *         false otherwise.
 *
 * @see RFC 4648 - The Base64 Alphabet: https://datatracker.ietf.org/doc/html/rfc4648
 */
[[nodiscard]] bool is_base64(std::string_view sv);
/**
 * @brief Decodes a Base64-encoded string into binary data.
 *
 * Assumes that the input is a valid Base64 string (i.e., only valid characters,
 * correct length, and padding). No error checking is performed.
 *
 * @param input The Base64-encoded input string.
 * @return The decoded binary data as a std::string.
 *
 * @see RFC 4648 - The Base64 Alphabet: https://datatracker.ietf.org/doc/html/rfc4648
 */
[[nodiscard]] std::string decode_base64(std::string_view input);
} // namespace jogasaki::utils
