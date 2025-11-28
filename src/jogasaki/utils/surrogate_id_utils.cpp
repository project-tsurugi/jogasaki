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
#include <jogasaki/utils/surrogate_id_utils.h>

#include <cstdint>
#include <string>
#include <string_view>

#include <jogasaki/utils/assert.h>

namespace jogasaki::utils {

std::string to_big_endian(std::uint64_t in) {
    constexpr std::size_t sz = sizeof(in);
    std::string result(sz, '\0');
    for (std::size_t i = 0; i < sz; ++i) {
        result[sz - 1 - i] = static_cast<char>(in & 0xFFU);
        in >>= 8U;
    }
    return result;
}

std::uint64_t from_big_endian(std::string_view bytes) {
    constexpr std::size_t sz = sizeof(std::uint64_t);
    assert_with_exception(bytes.size() == sz, bytes.size());
    std::uint64_t result = 0;
    for (std::size_t i = 0; i < sz; ++i) {
        result = (result << 8U) | static_cast<unsigned char>(bytes[i]);
    }
    return result;
}

bool is_surrogate_id(std::string_view storage_key) {
    // currently we assume surrogate_id is 8 bytes sequence with most significant byte is 0x00
    constexpr std::size_t sz = sizeof(std::uint64_t);
    return storage_key.size() == sz && storage_key[0] == 0;
}

}  // namespace jogasaki::utils
