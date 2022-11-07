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

#include <jogasaki/utils/coder.h>
#include <jogasaki/utils/decimal.h>

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

void writable_stream::write_decimal(std::int8_t sign, std::uint64_t lo, std::uint64_t hi, std::size_t sz, order odr) {
    if (capacity_ < pos_ + sz) {
        if(! ignore_overflow_) fail();
        pos_ += sz;
        return;
    }
    bool msb_inverted = false;
    if (sz > sizeof(std::uint64_t) * 2) {
        // write sign bit
        auto ch = static_cast<std::uint8_t>(sign >= 0 ? '\x00' : '\xFF');
        ch ^= details::SIGN_BIT<8>;
        *(base_ + pos_) = odr == order::ascending ? ch : ~ch;  // NOLINT
        ++pos_;
        --sz;
        msb_inverted = true;
    }

    for (std::size_t offset = 0, n = std::min(sz, sizeof(std::uint64_t)); offset < n; ++offset) {
        auto ch = static_cast<std::uint8_t>(lo >> (offset * 8U));
        if (offset == sz-1 && !msb_inverted) {
            ch ^= details::SIGN_BIT<8>;
            msb_inverted = true;
        }
        *(base_ + pos_ + sz - offset - 1) = static_cast<std::int8_t>(odr == order::ascending ? ch : ~ch);  //NOLINT
    }
    if (sz > sizeof(std::uint64_t)) {
        for (std::size_t offset = 0, n = std::min(sz - sizeof(std::uint64_t), sizeof(std::uint64_t)); offset < n; ++offset) {
            auto ch = static_cast<std::uint8_t>(hi >> (offset * 8U));
            if (offset+sizeof(std::uint64_t) == sz-1 && !msb_inverted) {
                ch ^= details::SIGN_BIT<8>;
                msb_inverted = true;
            }
            *(base_ + pos_ + sz - offset - sizeof(std::uint64_t) - 1) = static_cast<std::int8_t>(odr == order::ascending ? ch : ~ch);  //NOLINT
        }
    }
    pos_ += sz;
}

void decimal_error_logging(std::string_view operation, runtime_t<meta::field_type_kind::decimal> data, std::size_t precision, std::size_t scale, std::size_t digits) {
    VLOG(log_error) << "decimal operation (" << operation << ") failed. src=" << data << " precision= " << precision << " scale=" << scale << " digits=" << digits;
}

status writable_stream::do_write(runtime_t<meta::field_type_kind::decimal> data, order odr, std::size_t precision, std::size_t scale) {
    auto sz = utils::bytes_required_for_digits(precision);
    decimal::Decimal x{data};
    if((decimal::context.status() & MPD_IEEE_Invalid_operation) != 0) {
        decimal_error_logging("value creation", data, precision, scale, -1);
        return status::err_expression_evaluation_failure; // TODO
    }
    decimal::context.clear_status();
    auto y = x.rescale(-static_cast<std::int64_t>(scale));
    if((decimal::context.status() & MPD_Inexact) != 0) {
        decimal_error_logging("rescale", data, precision, scale, -1);
        return status::err_expression_evaluation_failure; // TODO
    }
    auto digits = y.get()->digits;
    if(static_cast<std::int64_t>(precision) < digits) {
        decimal_error_logging("digits", data, precision, scale, digits);
        return status::err_expression_evaluation_failure; // TODO
    }
    takatori::decimal::triple tri{y};
    auto [hi, lo, s] = utils::make_signed_coefficient_full(tri);
    (void)s;
    write_decimal(data.sign(), lo, hi, sz, odr);
    return status::ok;
}

}

