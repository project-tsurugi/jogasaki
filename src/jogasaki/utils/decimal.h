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

#include <initializer_list>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/data/any.h>

namespace jogasaki::utils {

/**
 * @brief create signed coefficient from triple
 * @param value source triple
 * @return `hi`, `lo` and `sz` return values:
 * hi - more significant 64-bit
 * lo - less significant 64-bit
 * sz - min size in bytes to represent the signed coefficient. The `sz` bytes from the least significant byte in
 * result 128-bit (concatenated `hi` with `lo`) represents the result. For valid input, `sz` ranges from 1 to 17.
 * The `sz` being 17 is the special case where most significant byte (not part of `hi` or `lo`) is 0x00 or 0xFF
 * to represent only sign.
 * @note even when `sz` is less than 16 or 8, bytes outside `sz` length (in concatednated `hi` and `lo`) is valid and
 * `lo` or concatenated `hi` and `lo` can be used to represent the signed 64-bit or 128-bit coefficient.
 * @note if `sz` is 17, the most significant byte is not provided by `hi` and `lo`. Caller should check the sign of
 * input `value` to determine the most significant byte.
 * @note this function is partly borrowed from original in value_output.cpp, and extended to support
 * decimal values whose coeffient is smaller than 64-bit.
 */
std::tuple<std::uint64_t, std::uint64_t, std::size_t> make_signed_coefficient_full(takatori::decimal::triple value);

/**
 * @brief validate the decimal data in the buffer
 * @param buf the data to validate
 * @return true if buffer has valid decimal coefficient
 * @return false otherwise
 */
bool validate_decimal_coefficient(std::string_view buf);

/**
 * @brief read decimal from the buffer and return triple
 * @param data the decimal data to read
 * @param scale the scale of the result decimal
 * @return the triple to represent the decimal data
 */
takatori::decimal::triple read_decimal(std::string_view data, std::size_t scale);

constexpr std::size_t max_decimal_length = sizeof(std::uint64_t) * 2 + 1;

using decimal_buffer = std::array<std::uint8_t, max_decimal_length>;

/**
 * @brief write decimal to the buffer
 * @param triple the decimal triple to write to the output buffer
 * @param out the output buffer
 */
void create_decimal(
    std::int8_t sign,
    std::uint64_t lo,
    std::uint64_t hi,
    std::size_t sz,
    decimal_buffer& out
);

}


