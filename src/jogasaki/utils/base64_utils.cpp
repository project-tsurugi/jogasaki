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
#include "base64_utils.h"
#include <array>
#include <iostream>
namespace jogasaki::utils {

// X'6162'
// 01100001 01100010
// buffer = (buffer << 8) | c;
// buffer = 00000000 00000000 00000000 01100001
// buffered_bits += 8;
// buffered_bits = 2
// (buffer >> buffered_bits) & base64_mask
// ( 00000000 00000000 00000000 01100001 >> 2 ) & 0b0011'1111
//   00000000 00000000 00000000 00011000 & 0b0011'1111
//   00000000 00000000 00000000 00011000
//   table[24] =Y
// buffered_bits = -4
// buffer = (buffer << 8) | c;
// buffer = 00000000 00000000 01100001 01100010
// buffered_bits += 8;
// buffered_bits = 4
// (buffer >> buffered_bits) & base64_mask
// ( 00000000 00000000 01100001 01100010 >> 4 ) & 0b0011'1111
//   00000000 00000000 00000110 00010110 & 0b0011'1111
//   00000000 00000000 00000000 00010110
//   table[22] = W
// buffered_bits = -2
// ((buffer << 8) >> (buffered_bits + 8)) & base64_mask
// ((00000000 00000000 01100001 01100010 << 8 ) >> 6) & 0b0011'1111
//  (00000000 01100001 01100010 00000000 >> 6) & 0b0011'1111
//   00000000 00000001 10000101 10001000 & 0b0011'1111
//   00000000 00000000 00000000 00001000
//   table[8] = I
//  while (output.size() % 4) {
//      output.push_back('=');
//  }
//  output.size() =3
//  ∴ YWI=
[[nodiscard]] std::string encode_base64(std::string_view input) {
    static constexpr std::array<char, 64> table = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
        'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b',
        'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
        'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};

    constexpr std::size_t base64_mask = 0x3F;
    std::string output;
    // Base64 encoding expands data from 3 bytes to 4 characters.
    // If the input size is not a multiple of 3, padding is added,
    // so the output size is always a multiple of 4.
    std::size_t estimated_size = ((input.size() + 2) / 3) * 4;
    output.reserve(estimated_size);
    unsigned int buffer = 0;
    int buffered_bits   = -6;

    for (unsigned char c : input) {
        // insert 8 bits into buffer
        buffer = (buffer << 8) | c; // NOLINT(hicpp-signed-bitwise)
        buffered_bits += 8;
        while (buffered_bits >= 0) {
            auto index = static_cast<std::size_t>(
                (buffer >> buffered_bits) & base64_mask); // NOLINT(hicpp-signed-bitwise)
            output.push_back(table.at(index));
            buffered_bits -= 6;
        }
    }

    if (buffered_bits > -6) {
        auto shift_amount = static_cast<std::uint32_t>(buffered_bits + 8);
        auto index        = static_cast<std::size_t>(
            ((buffer << 8) >> shift_amount) & base64_mask); // NOLINT(hicpp-signed-bitwise)
        output.push_back(table.at(index));
    }
    // padding
    while ((output.size() % 4) != 0) {
        output.push_back('=');
    }
    return output;
}

} // namespace jogasaki::utils
