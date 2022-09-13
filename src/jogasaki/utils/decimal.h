/*
 * Copyright 2018-2020 tsurugi project.
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
 * @brief create signed coefficient from triple that has unsigned components
 * @param value source triple
 * @return `hi`, `lo` and `sz` return values:
 * hi - more significant 64-bit
 * lo - less significant 64 bit
 * sz - min size in bytes to represent the signed coefficient. The `sz` bytes from the least significant byte in
 * result 128-bit (concatenated `hi` with `lo`) represents the result. For valid input, `sz` ranges from 1 to 17.
 * The `sz` being 17 is the special case where most significant byte (not part of `hi` or `lo`) is 0x00 or 0xFF
 * to represent only sign.
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

}

