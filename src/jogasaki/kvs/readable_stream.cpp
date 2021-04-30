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

}

