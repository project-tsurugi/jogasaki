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
#include "writable_stream.h"

#include <cmath>
#include <array>
#include <map>
#include <boost/endian/conversion.hpp>
#include <takatori/decimal/triple.h>
#include <decimal.hh>

#include "readable_stream.h"

namespace jogasaki::kvs {


writable_stream::writable_stream(void* buffer, std::size_t capacity, bool ignore_overflow) :
    base_(static_cast<char*>(buffer)),
    capacity_(capacity),
    ignore_overflow_(ignore_overflow)
{}

writable_stream::writable_stream(std::string& s, bool ignore_overflow) :
    writable_stream(s.data(), s.capacity(), ignore_overflow)
{}

status writable_stream::write(char const* dt, std::size_t sz) {
    if (sz == 0) {
        return status::ok;
    }
    if (pos_ + sz > capacity_) {
        if(! ignore_overflow_) {
            fail();
        }
    } else {
        std::memcpy(base_ + pos_, dt, sz);  // NOLINT
    }
    pos_ += sz;
    return status::ok;
}

void writable_stream::reset() {
    pos_ = 0;
}

std::size_t writable_stream::size() const noexcept {
    return pos_;
}

std::size_t writable_stream::capacity() const noexcept {
    return capacity_;
}

char const* writable_stream::data() const noexcept {
    return base_;
}

readable_stream writable_stream::readable() const noexcept {
    return readable_stream{base_, capacity_};
}

void writable_stream::do_write(char const* dt, std::size_t sz, order odr) {
    if (sz == 0) {
        return;
    }
    if (pos_ + sz > capacity_) {
        if(! ignore_overflow_) {
            fail();
        }
    } else {
        if (odr == order::ascending) {
            std::memcpy(base_ + pos_, dt, sz);  // NOLINT
        } else {
            for (std::size_t i = 0; i < sz; ++i) {
                *(base_ + pos_ + i) = ~(*(dt + i));  // NOLINT
            }
        }
    }
    pos_ += sz;
}

void writable_stream::do_write(char ch, std::size_t sz, order odr) {
    BOOST_ASSERT(capacity_ == 0 || pos_ + sz <= capacity_);  // NOLINT
    if (sz == 0) {
        return;
    }
    if (pos_ + sz > capacity_) {
        if(! ignore_overflow_) {
            fail();
        }
    } else {
        for (std::size_t i = 0; i < sz; ++i) {
            *(base_ + pos_ + i) = odr == order::ascending ? ch : ~ch;  // NOLINT
        }
    }
    pos_ += sz;
}

void writable_stream::ignore_overflow(bool arg) noexcept {
    ignore_overflow_ = arg;
}

constexpr std::size_t max_decimal_digits = 38;

std::array<std::size_t, max_decimal_digits> init_digits_map() {
    std::array<std::size_t, max_decimal_digits> ret{};
    std::map<std::size_t, std::size_t> digits_to_bytes{};
    auto log2 = std::log10(2.0);
    digits_to_bytes.emplace(0, 0);
    for(std::size_t i=1; i < max_decimal_digits; ++i) {
        digits_to_bytes.emplace(std::floor((8*i-1) * log2), i);
    }
    for(std::size_t i=0; i < max_decimal_digits; ++i) {
        for(auto [a, b]: digits_to_bytes) {
            if(a >= i) {
                ret[i] = b;
                break;
            }
        }
    }
    return ret;
}

std::size_t bytes_required_for_digits(std::size_t digits) {
    static std::array<std::size_t, max_decimal_digits> arr = init_digits_map();
    return arr[digits];
}

void writable_stream::write_variable_integer(std::uint64_t lo, std::uint64_t hi, std::size_t sz, order odr) {
    for (std::size_t offset = 0, n = std::min(sz, sizeof(std::uint64_t)); offset < n; ++offset) {
        auto ch = static_cast<char>(lo >> (offset * 8U));
        if (offset == sz-1) {
            ch ^= details::SIGN_BIT<8>;
        }
        *(base_ + pos_ + sz - offset - 1) = odr == order::ascending ? ch : ~ch;
    }
    if (sz > sizeof(std::uint64_t)) {
        for (std::size_t offset = 0, n = std::min(sz - sizeof(std::uint64_t), sizeof(std::uint64_t)); offset < n; ++offset) {
            auto ch = static_cast<char>(hi >> (offset * 8U));
            if (offset+sizeof(std::uint64_t) == sz-1) {
                ch ^= details::SIGN_BIT<8>;
            }
            *(base_ + pos_ + sz - offset - sizeof(std::uint64_t) - 1) = odr == order::ascending ? ch : ~ch;
        }
    }
    pos_ += sz;
}

static std::tuple<std::uint64_t, std::uint64_t, std::size_t> make_signed_coefficient_full(takatori::decimal::triple value) {
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

status writable_stream::do_write(runtime_t<meta::field_type_kind::decimal> data, order odr, std::size_t precision, std::size_t scale) {
    auto sz = bytes_required_for_digits(precision);
    BOOST_ASSERT(capacity_ == 0 || pos_ + sz <= capacity_);  // NOLINT
    auto ctx = decimal::IEEEContext(128);
    decimal::Decimal x{data};
    if(decimal::context.status() & MPD_IEEE_Invalid_operation) {
        return status::err_expression_evaluation_failure; // TODO
    }
    decimal::context.clear_status();
    auto y = x.rescale(-scale);
    if(decimal::context.status() & MPD_Inexact) {
        return status::err_expression_evaluation_failure; // TODO
    }
    auto digits = y.get()->digits;
    if(static_cast<std::int64_t>(precision) < digits) {
        return status::err_expression_evaluation_failure; // TODO
    }
    takatori::decimal::triple tri{y};
    auto [hi, lo, s] = make_signed_coefficient_full(tri);
    (void)s;
    write_variable_integer(lo, hi, sz, odr);
    return status::ok;
}

}

