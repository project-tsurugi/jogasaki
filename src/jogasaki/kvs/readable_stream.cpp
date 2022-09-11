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
#include "readable_stream.h"

#include <boost/endian/conversion.hpp>
#include <jogasaki/utils/coder.h>

#include "coder.h"

namespace jogasaki::kvs {

readable_stream::readable_stream(void const* buffer, std::size_t capacity) :
    base_(static_cast<char const*>(buffer)),
    capacity_(capacity)
{}

readable_stream::readable_stream(std::string& s) :
    readable_stream(s.data(), s.capacity())
{}

void readable_stream::reset() {
    pos_ = 0;
}

std::size_t readable_stream::size() const noexcept {
    return pos_;
}

std::size_t readable_stream::capacity() const noexcept {
    return capacity_;
}

char const* readable_stream::data() const noexcept {
    return base_;
}

std::string_view readable_stream::rest() const noexcept {
    return {base_+pos_, capacity_-pos_};  //NOLINT
}

constexpr std::size_t max_decimal_coefficient_size = sizeof(std::uint64_t) * 2 + 1;

std::string_view read_decimal_coefficient(
    order odr,
    std::string_view buffer,
    std::size_t sz,
    std::array<char, max_decimal_coefficient_size>& out
) {
    for(std::size_t i=0; i < sz; ++i) {
        char ch = i==0 ? buffer[i] ^ details::SIGN_BIT<8> : buffer[i];
        out[i] = odr == order::ascending ? ch : ~ch;
    }
    std::string_view buf{out.data(), sz};
    if (sz != max_decimal_coefficient_size) {
        return buf;
    }

    auto first = static_cast<std::uint8_t>(buf[0]);
    // positive is OK because coefficient is [0, 2^128)
    if (first == 0) {
        return buf;
    }

    if (first == 0xffU) {
        // check negative value to avoid -2^128 (0xff 0x00.. 0x00)
        auto const* found = std::find_if(
            buf.begin() + 1,
            buf.end(),
            [](auto c) { return c != '\0'; });
        if (found != buf.end()) {
            return buf;
        }
    }
    fail(); // TODO raise exception?
}

runtime_t<meta::field_type_kind::decimal>
readable_stream::do_read(order odr, bool discard, std::size_t precision, std::size_t scale) {
    auto sz = utils::bytes_required_for_digits(precision);
    BOOST_ASSERT(pos_ + sz <= capacity_);  // NOLINT
    std::array<char, max_decimal_coefficient_size> buf{0};
    auto data = read_decimal_coefficient(odr, {base_+pos_, capacity_}, sz, buf);
    pos_ += sz;
    if (discard) {
        return {};
    }

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

}

