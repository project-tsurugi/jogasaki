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

[[nodiscard]] constexpr bool is_continuation_byte(unsigned char c) noexcept;

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
[[nodiscard]] std::size_t get_byte(encoding_type e) noexcept;

} // namespace jogasaki::utils
