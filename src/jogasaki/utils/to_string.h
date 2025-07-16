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

#include <sstream>
#include <iomanip>

namespace jogasaki::utils {

/**
 * @brief convert values to string
 * @details this function is intended to replace std::to_string for floating point values. The function std::to_string
 * converts floating point values by sprintf with %f, and the string representation seems to have several problems
 * (e.g. trailing zeros, too many digits, etc.)
*/
template <class T>
std::string to_string(T const& arg) {
    std::stringstream ss{};
    ss << std::uppercase;
    ss << arg;
    return ss.str();
}

}  // namespace jogasaki::utils

