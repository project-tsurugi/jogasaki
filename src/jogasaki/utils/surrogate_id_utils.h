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

#include <cstdint>
#include <string>
#include <string_view>

namespace jogasaki::utils {

/**
 * @brief encode input value to big-endian binary string
 * @param in the input value to encode
 * @return the 8-byte big-endian binary representation
 */
std::string to_big_endian(std::uint64_t in);

/**
 * @brief decode unsigned integer value from big-endian binary string
 * @param bytes the byte sequence containing the unsigned integer in big-endian format
 * @return the decoded integer value
 * @pre bytes.size() == sizeof(std::uint64_t)
 */
std::uint64_t from_big_endian(std::string_view bytes);

/**
 * @brief check if the storage key represents a surrogate ID
 * @param storage_key the storage key to check
 * @return true if the storage key is 8 bytes (surrogate ID format)
 * @return false otherwise (index name format)
 */
bool is_surrogate_id(std::string_view storage_key);

}  // namespace jogasaki::utils
