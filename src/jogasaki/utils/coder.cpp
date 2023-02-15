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
#include "coder.h"

#include <cmath>
#include <map>
#include <array>
#include <takatori/util/exception.h>

#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/data/any.h>

namespace jogasaki::utils {

using takatori::util::throw_exception;

status encode_any(
    data::aligned_buffer& target,
    meta::field_type const& type,
    bool nullable,
    kvs::coding_spec spec,
    std::initializer_list<data::any> sources
) {
    std::size_t length = 0;
    for(int loop = 0; loop < 2; ++loop) { // if first trial overflows `buf`, extend it and retry
        kvs::writable_stream s{target.data(), target.capacity(), loop == 0};
        for(auto&& f : sources) {
            if (f.empty()) {
                // value not specified for the field
                if (! nullable) {
                    throw_exception(std::logic_error{""});
                }
                if(auto res = kvs::encode_nullable(f, type, spec, s); res != status::ok) {
                    return res;
                }
            } else {
                if (nullable) {
                    if(auto res = kvs::encode_nullable(f, type, spec, s); res != status::ok) {
                        return res;
                    }
                } else {
                    if(auto res = kvs::encode(f, type, spec, s); res != status::ok) {
                        return res;
                    }
                }
            }
        }
        length = s.size();
        bool fit = length <= target.capacity();
        target.resize(length);
        if (loop == 0) {
            if (fit) {
                break;
            }
            target.resize(0); // set data size 0 and start from beginning
        }
    }
    return status::ok;
}

constexpr std::size_t max_decimal_digits = 38;

auto init_digits_map() {
    std::array<std::size_t, max_decimal_digits+2> ret{};
    std::map<std::size_t, std::size_t> digits_to_bytes{};
    auto log2 = std::log10(2.0);
    digits_to_bytes.emplace(0, 0);
    for(std::size_t i=1; i < 16+2; ++i) {
        digits_to_bytes.emplace(std::floor(static_cast<double>(8*i-1) * log2), i);
    }
    for(std::size_t i=0, n=ret.size(); i < n; ++i) {
        for(auto [a, b]: digits_to_bytes) {
            if(a >= i) {
                ret.at(i) = b;
                break;
            }
        }
    }
    return ret;
}

std::size_t bytes_required_for_digits(std::size_t digits) {
    static auto arr = init_digits_map();
    BOOST_ASSERT(digits < arr.size());  //NOLINT
    return arr.at(digits);
}

}

