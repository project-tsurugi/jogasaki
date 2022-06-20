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
#include <boost/endian/conversion.hpp>

#include "readable_stream.h"

namespace jogasaki::kvs {


writable_stream::writable_stream(void* buffer, std::size_t capacity) :
    base_(static_cast<char*>(buffer)),
    capacity_(capacity)
{}

writable_stream::writable_stream(std::string& s) : writable_stream(s.data(), s.capacity()) {}

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

}

