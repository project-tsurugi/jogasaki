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
#pragma once

#include <string>
#include <string_view>

namespace jogasaki::utils {

/**
 * @brief Sanitize UTF-8 input (may be ill-formed) and produce a safe, printable UTF-8 string.
 *
 * This function validates UTF-8 sequences and escapes invalid or non-printable bytes.
 * - C0 control characters (U+0000-U+001F) are escaped
 * - DEL character (U+007F) is escaped
 * - C1 control characters (U+0080-U+009F) are escaped
 * - Invalid UTF-8 byte sequences (including incomplete sequences, overlong encodings,
 *   UTF-16 surrogates, and out-of-range code points) are escaped byte by byte
 * - Valid printable UTF-8 sequences are preserved as-is
 *
 * @param str Input text (may contain ill-formed UTF-8)
 * @return A valid UTF-8 string where non-printable and invalid bytes are escaped in \x{HH} format.
 */
std::string sanitize_utf8(std::string_view str);

}

