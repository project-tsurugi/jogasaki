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
#include "text.h"

#include <cassert>

namespace jogasaki::accessor {

text::text(memory::paged_memory_resource *resource, const char *data, text::size_type size) { //NOLINT
    if (size <= short_text::max_size) {
        s_ = short_text(data, size);  //NOLINT(cppcoreguidelines-pro-type-union-access)
        return;
    }
    auto p = resource->allocate(size, 1);
    std::memcpy(p, data, size);
    l_ = long_text(static_cast<char*>(p), size);  //NOLINT(cppcoreguidelines-pro-type-union-access)
}

text::text(memory::paged_memory_resource *resource, std::string_view str) : text(resource, str.data(), str.size()) {}

text::operator std::string_view() const noexcept {
    if (is_short()) {
        return {s_.data(), s_.size()};  //NOLINT(cppcoreguidelines-pro-type-union-access)
    }
    return {l_.data(), l_.size()};  //NOLINT(cppcoreguidelines-pro-type-union-access)
}

bool text::is_short() const noexcept {
    return s_.is_short();  //NOLINT(cppcoreguidelines-pro-type-union-access)
}

int compare(const text &a, const text &b) noexcept {
    std::string_view sv_a{a};
    std::string_view sv_b{b};
    return sv_a.compare(sv_b);
}

bool operator<(const text &a, const text &b) noexcept {
    return compare(a, b) < 0;
}

bool operator>(const text &a, const text &b) noexcept {
    return compare(a, b) > 0;
}

bool operator<=(const text &a, const text &b) noexcept {
    return compare(a, b) <= 0;
}

bool operator>=(const text &a, const text &b) noexcept {
    return compare(a, b) >= 0;
}

bool operator==(const text &a, const text &b) noexcept {
    std::string_view sv_a{a};
    std::string_view sv_b{b};
    return sv_a == sv_b;
}

bool operator!=(const text &a, const text &b) noexcept {
    return !(a == b);
}

text::long_text::long_text(char *allocated_data, text::size_type size) noexcept
        : data_(allocated_data)
        , size_(size & max_size)
{
    assert(size <= max_size); // NOLINT
}

char const *text::long_text::data() const noexcept {
    return data_;
}

text::size_type text::long_text::size() const noexcept {
    return size_;
}



text::short_text::short_text(const char *data, text::short_text::short_size_type size) noexcept // NOLINT
        : size_and_is_short_(size | is_short_mask)
{
    assert(size <= max_size); // NOLINT
    std::memcpy(&data_[0], data, size & size_mask);
}

bool text::short_text::is_short() const noexcept {
    return (size_and_is_short_ & is_short_mask) != 0;
}

char const *text::short_text::data() const noexcept {
    return &data_[0];
}

text::size_type text::short_text::size() const noexcept {
    return size_and_is_short_ & size_mask;
}
}
