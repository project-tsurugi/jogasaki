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
#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace jogasaki::utils {

/**
 * @brief convert hexadecimal string to octet string
 * @param src hexadecimal string
 * @param out [out] octet string
 * @return true if success
 * @return false on error
 */
bool hex_to_octet(std::string_view src, std::string& out) noexcept;

}  // namespace jogasaki::utils
