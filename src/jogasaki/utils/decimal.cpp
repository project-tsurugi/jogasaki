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
#include "decimal.h"

#include <initializer_list>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/data/any.h>

namespace jogasaki::utils {

std::tuple<std::uint64_t, std::uint64_t, std::size_t> make_signed_coefficient_full(takatori::decimal::triple value) {
    std::uint64_t c_hi = value.coefficient_high();
    std::uint64_t c_lo = value.coefficient_low();

    if (value.sign() >= 0) {
        for (std::size_t offset = 0; offset < sizeof(std::uint64_t); ++offset) {
            std::uint64_t octet = (c_hi >> ((sizeof(std::uint64_t) - offset - 1U) * 8U)) & 0xffU;
            if (octet != 0) {
                std::size_t size { sizeof(std::uint64_t) * 2 - offset };
                if ((octet & 0x80U) != 0) {
                    ++size;
                }
                return { c_hi, c_lo, size };
            }
        }
        return { c_hi, c_lo, sizeof(std::uint64_t) + 1 };
    }

    // for negative numbers

    if (value.sign() < 0) {
        c_lo = ~c_lo + 1;
        c_hi = ~c_hi;
        if (c_lo == 0) {
            c_hi += 1; // carry up
        }
    }

    for (std::size_t offset = 0; offset < sizeof(std::uint64_t); ++offset) {
        std::uint64_t octet = (c_hi >> ((sizeof(std::uint64_t) - offset - 1U) * 8U)) & 0xffU;
        if (octet != 0xffU) {
            std::size_t size { sizeof(std::uint64_t) * 2 - offset };
            if ((octet & 0x80U) == 0) {
                ++size;
            }
            return { c_hi, c_lo, size };
        }
    }
    return { c_hi, c_lo, sizeof(std::uint64_t) + 1 };
}
}

