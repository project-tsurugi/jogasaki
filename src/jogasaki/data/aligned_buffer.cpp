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
#include "aligned_buffer.h"

#include <cstring>

#include <jogasaki/utils/binary_printer.h>

namespace jogasaki::data {

aligned_buffer::aligned_buffer(
    std::size_t size,
    std::size_t align
) noexcept :
    capacity_(size),
    alignment_(align)
{}

std::size_t aligned_buffer::size() const noexcept {
    return capacity_;
}

void* aligned_buffer::data() const noexcept {
    return data_.get();
}

aligned_buffer::operator bool() const noexcept {
    return !empty();
}

[[nodiscard]] bool aligned_buffer::empty() const noexcept {
    return capacity_ == 0;
}

void aligned_buffer::resize(std::size_t sz) {
    auto n = utils::make_aligned_array<std::byte>(alignment_, sz);
    data_.swap(n);
    capacity_ = sz;
}

std::size_t aligned_buffer::alignment() const noexcept {
    return alignment_;
}

bool operator==(aligned_buffer const& a, aligned_buffer const& b) noexcept {
    return a.data_ == b.data_;
}

bool operator!=(aligned_buffer const& a, aligned_buffer const& b) noexcept {
    return !(a == b);
}

std::ostream& operator<<(std::ostream& out, aligned_buffer const& value) {
    out << " capacity: " << value.size()
        << " alignment: " << value.alignment()
        << " data: " << utils::binary_printer{value.data_.get(), value.size()};
    return out;
}

aligned_buffer::aligned_buffer(aligned_buffer const& other) :
    capacity_(other.capacity_),
    alignment_(other.alignment_),
    data_(utils::make_aligned_array<std::byte>(alignment_, capacity_))
{
    std::memcpy(data_.get(), other.data_.get(), capacity_);
}

aligned_buffer& aligned_buffer::operator=(aligned_buffer const& other) {
    capacity_ = other.capacity_;
    alignment_ = other.alignment_;
    data_ = utils::make_aligned_array<std::byte>(alignment_, capacity_);
    std::memcpy(data_.get(), other.data_.get(), capacity_);
    return *this;
}

aligned_buffer::operator std::string_view() const noexcept {
    return {reinterpret_cast<char*>(data_.get()), capacity_};  //NOLINT
}

aligned_buffer::aligned_buffer(std::string_view s) :
    aligned_buffer(s.size())
{
    if (! s.empty()) {
        std::memcpy(data_.get(), s.data(), s.size());
    }
}

} // namespace
