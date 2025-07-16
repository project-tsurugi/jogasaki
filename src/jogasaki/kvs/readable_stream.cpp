/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <array>
#include <cstddef>

#include <jogasaki/utils/base_filename.h>
#include <jogasaki/utils/coder.h>
#include <jogasaki/utils/decimal.h>

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

std::string_view process_order_and_msb(
    order odr,
    std::string_view buffer,
    std::size_t sz,
    std::array<std::uint8_t, max_decimal_coefficient_size>& out
) {
    for(std::size_t i=0; i < sz; ++i) {
        std::uint8_t ch = i==0 ? static_cast<std::uint8_t>(buffer[i]) ^ details::SIGN_BIT<8> : buffer[i];
        out.at(i) = odr == order::ascending ? ch : ~ch;
    }
    return std::string_view{reinterpret_cast<char*>(out.data()), sz};  //NOLINT
}


std::string_view read_decimal_coefficient(
    order odr,
    std::string_view buffer,
    std::size_t sz,
    std::array<std::uint8_t, max_decimal_coefficient_size>& out
) {
    auto buf = process_order_and_msb(odr, buffer, sz, out);
    if(utils::validate_decimal_coefficient({buf.data(), sz})) {
        return buf;
    }
    throw_exception(std::domain_error{"invalid decimal data"});
}

runtime_t<meta::field_type_kind::decimal>
readable_stream::do_read(order odr, bool discard, std::size_t precision, std::size_t scale) {
    auto sz = utils::bytes_required_for_digits(precision);
    if(!(pos_ + sz <= capacity_)) throw_exception(std::domain_error{  //NOLINT
            string_builder{} << base_filename() << " condition pos_ + sz <= capacity_ failed with pos_:" << pos_ << " sz:" << sz << " capacity_:" << capacity_ << string_builder::to_string
        });
    std::array<std::uint8_t, max_decimal_coefficient_size> buf{0};
    auto data = read_decimal_coefficient(odr, {base_+pos_, capacity_}, sz, buf); //NOLINT
    pos_ += sz;
    if (discard) {
        return {};
    }
    return utils::read_decimal(data, scale);
}

}

