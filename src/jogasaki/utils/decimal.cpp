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
#include "decimal.h"

#include <algorithm>
#include <limits>
#include <utility>
#include <boost/assert.hpp>

namespace jogasaki::utils {

constexpr static std::size_t npos = static_cast<std::size_t>(-1);

/**
 * @brief find the most significant non-zero byte offset in the 64-bit value
 * @param v the source value to check
 * @param zero what should be treated as "zero" byte, specify 0x00UL (for positive value) or 0xFFUL (for negative value)
 * @return the byte offset (ranging from 0 to 7) of the most significant non-zero byte in `v`
 * and the flag to indicate if the most significant bit is "non-zero"
 */
std::pair<std::size_t, bool> most_significant_non_zero_byte_offset(std::uint64_t v, std::uint64_t zero) {
    for (std::size_t offset = 0; offset < sizeof(std::uint64_t); ++offset) {
        std::uint64_t octet = (v >> ((sizeof(std::uint64_t) - offset - 1U) * 8U)) & 0xffU;
        if (octet != zero) {
            if ((octet & 0x80U) != (zero & 0x80U)) {
                return {offset, true};
            }
            return {offset, false};
        }
    }
    return {npos, false};
}

std::tuple<std::uint64_t, std::uint64_t, std::size_t> make_signed_coefficient_full(takatori::decimal::triple value) {
    std::uint64_t c_hi = value.coefficient_high();
    std::uint64_t c_lo = value.coefficient_low();

    if (value.sign() >= 0) {
        if(auto [offset, msb] = most_significant_non_zero_byte_offset(c_hi, 0); offset != npos) {
            std::size_t size { sizeof(std::uint64_t) * 2 - offset };
            if(msb) {
                ++size;
            }
            return { c_hi, c_lo, size };
        }
        if(auto [offset, msb] = most_significant_non_zero_byte_offset(c_lo, 0); offset != npos) {
            std::size_t size { sizeof(std::uint64_t) - offset };
            if(msb) {
                ++size;
            }
            return { c_hi, c_lo, size };
        }
        // all bits unset (i.e. zero)
        return { c_hi, c_lo, 1 };
    }

    // for negative numbers

    if (value.sign() < 0) {
        c_lo = ~c_lo + 1;
        c_hi = ~c_hi;
        if (c_lo == 0) {
            c_hi += 1; // carry up
        }
    }

    if(auto [offset, msb] = most_significant_non_zero_byte_offset(c_hi, 0xffU); offset != npos) {
        std::size_t size { sizeof(std::uint64_t) * 2 - offset };
        if(msb) {
            ++size;
        }
        return { c_hi, c_lo, size };
    }
    if(auto [offset, msb] = most_significant_non_zero_byte_offset(c_lo, 0xffU); offset != npos) {
        std::size_t size { sizeof(std::uint64_t) - offset };
        if(msb) {
            ++size;
        }
        return { c_hi, c_lo, size };
    }
    // all bits set (i.e. -1)
    return { c_hi, c_lo, 1 };
}

constexpr std::size_t max_decimal_coefficient_size = sizeof(std::uint64_t) * 2 + 1;

bool validate_decimal_coefficient(
    std::string_view buf
) {
    if(buf.size() < max_decimal_coefficient_size) {
        return true;
    }
    auto first = static_cast<std::uint8_t>(buf[0]);
    // positive is OK because coefficient is [0, 2^128)
    if (first == 0) {
        return true;
    }

    if (first == 0xffU) {
        // check negative value to avoid -2^128 (0xff 0x00.. 0x00)
        auto const* found = std::find_if(
            buf.begin() + 1,
            buf.end(),
            [](auto c) { return c != '\0'; });
        if (found != buf.end()) {
            return true;
        }
    }
    return false;
}

takatori::decimal::triple read_decimal(std::string_view data, std::size_t scale) {
    // extract lower 8-octets of coefficient
    std::uint64_t c_lo{};
    std::uint64_t shift{};
    for (std::size_t offset = 0;
        offset < data.size() && offset < sizeof(std::uint64_t);
        ++offset) {
        auto pos = data.size() - offset - 1;
        std::uint64_t octet { static_cast<std::uint8_t>(data[pos]) };
        c_lo |= octet << shift;
        shift += 8;
    }

    // extract upper 8-octets of coefficient
    std::uint64_t c_hi {};
    shift = 0;
    for (
        std::size_t offset = sizeof(std::uint64_t);
        offset < data.size() && offset < sizeof(std::uint64_t) * 2;
        ++offset) {
        auto pos = data.size() - offset - 1;
        std::uint64_t octet { static_cast<std::uint8_t>(data[pos]) };
        c_hi |= octet << shift;
        shift += 8;
    }

    bool negative = (static_cast<std::uint8_t>(data[0]) & 0x80U) != 0;

    if (negative) {
        // sign extension
        if (data.size() < sizeof(std::uint64_t) * 2) {
            auto mask = std::numeric_limits<std::uint64_t>::max(); // 0xfff.....
            if(data.size() < sizeof(std::uint64_t)) {
                std::size_t rest = data.size() * 8U;
                c_lo |= mask << rest;
                c_hi = mask;
            } else {
                std::size_t rest = (data.size() - sizeof(std::uint64_t)) * 8U;
                c_hi |= mask << rest;
            }
        }

        c_lo = ~c_lo + 1;
        c_hi = ~c_hi;
        if (c_lo == 0) {
            c_hi += 1; // carry up
        }
        // if negative, coefficient must not be zero
        BOOST_ASSERT(c_lo != 0 || c_hi != 0); // NOLINT
    }

    return takatori::decimal::triple{
        negative ? -1 : +1,
        c_hi,
        c_lo,
        -static_cast<std::int32_t>(scale),
    };
}

void create_decimal(
    std::int8_t sign,
    std::uint64_t lo,
    std::uint64_t hi,
    std::size_t sz,
    decimal_buffer& out
) {
    auto base_ = out.data();
    std::size_t pos_ = 0;

    if (sz > sizeof(std::uint64_t) * 2) {
        // write sign bit
        *(base_ + pos_) = sign >= 0 ? '\x00' : '\xFF';  // NOLINT
        ++pos_;
        --sz;
    }

    for (std::size_t offset = 0, n = std::min(sz, sizeof(std::uint64_t)); offset < n; ++offset) {
        *(base_ + pos_ + sz - offset - 1) = static_cast<char>(lo >> (offset * 8U));  //NOLINT
    }
    if (sz > sizeof(std::uint64_t)) {
        for (std::size_t offset = 0, n = std::min(sz - sizeof(std::uint64_t), sizeof(std::uint64_t)); offset < n; ++offset) {
            *(base_ + pos_ + sz - offset - sizeof(std::uint64_t) - 1) = static_cast<char>(hi >> (offset * 8U));  //NOLINT
        }
    }
}

}

