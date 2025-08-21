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
#include <cctype>
#include <regex>
#include <cstdint>
namespace jogasaki::utils {

// Step-by-Step Eecode Trace of "6162" to "YWI="
//
// Step 1: Encode 61(01100001)
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
//
// Step 2: Encode 62(01100010)
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
//  Step 3: Padding
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

[[nodiscard]] bool is_base64(std::string_view sv) {
    // The length of sv must be a multiple of 4
    if (sv.empty() || sv.size() % 4 != 0) { return false; }
    for (size_t i = 0; i < sv.size(); ++i) {
        auto c = static_cast<unsigned char>(sv[i]);
        // Valid Base64 characters: A-Z, a-z, 0-9, +, /
        if (static_cast<bool>(std::isalnum(c)) || c == '+' ||
            c == '/') {
            continue;
        }
        // Padding
        if (c == '=') {
            // Padding is allowed only at the end and must be at most 2 characters
            if (i < sv.size() - 2) return false;
            if (i == sv.size() - 2 && sv.back() != '=') return false;
            continue;
        }
        // invalid characters
        return false;
    }

    return true;
}

// Step-by-Step Decode Trace of "YUA=" to "0x61 0x40"
// input: YUA=
// Step 1: Decode Y (24)
// buffer = 00000000 00000000 00000000 00011000
// buffered_bits = 6
// Step 2: Decode U (20)
//          00000000 00000000 00000000 00011000 << 6 | 00010100
// buffer = 00000000 00000000 00000110 00010100
// buffered_bits = 12
// buffered_bits = 4
// 00000000 00000000 00000110 00010100 >> 4) & 0xFF
// 00000000 00000000 00000000 01100001 & 0xFF
//                            01100001
// 0x61
// Step 3: Decode U (0)
//          00000000 00000000 00000110 00010100 << 6 | 00000000
// buffer = 00000000 00000001 10000101 00000000
// buffered_bits = 10
// buffered_bits = 2
// 00000000 00000001 10000101 00000000 >> 2) & 0xFF
// 00000000 00000000 01100001 01000000 & 0xFF
//                            01000000
// 0x40
// Step 4: Decode '='
// Padding character, decoding stops here
// break
// ∴ 0x61 0x40
[[nodiscard]] std::string decode_base64(std::string_view input) {
    // See ASCII table: https://www.ascii-code.com/
    constexpr std::array<uint8_t, 123> decode_table = {
        // ASCII codes 0–42: all invalid (0xFF)
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 0–9
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 10–19
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 20–29
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 30–39
        0xFF, 0xFF, 0xFF,                                           // 40–42
        // 43 ('+')～47 ('/')
        62, 0xFF, 0xFF, 0xFF, 63, // 43–47
        // 48 ('0')～57 ('9')
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, // 48-57
        // 58～64 (: ; < = > ? @)
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 58–64
        // 65 ('A')～90 ('Z')
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,           // 65-74
        10, 11, 12, 13, 14, 15, 16, 17, 18, 19, // 75-84
        20, 21, 22, 23, 24, 25,                 // 85-90
        // 91～96 ([ \ ] ^ _ )
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 91-96
        // 97 ('a')～122 ('z')
        26, 27, 28, 29, 30, 31, 32, 33, 34, 35, // 97-106
        36, 37, 38, 39, 40, 41, 42, 43, 44, 45, // 107-116
        46, 47, 48, 49, 50, 51                  // 107-122
    };
    std::string output;
    output.reserve(input.size() * 3 / 4);

    unsigned int buffer = 0;
    int buffered_bits   = 0;

    for (unsigned char c : input) {
        if (c == '=') {
            break; // padding reached, stop decoding
        }
        auto val = decode_table.at(static_cast<std::size_t>(c));
        buffer   = (buffer << 6) | val; // NOLINT(hicpp-signed-bitwise)
        buffered_bits += 6;
        if (buffered_bits >= 8) {
            buffered_bits -= 8;
            auto index =
                static_cast<char>((buffer >> buffered_bits) & 0xFF); // NOLINT(hicpp-signed-bitwise)
            output.push_back(index);
        }
    }
    return output;
}

} // namespace jogasaki::utils
