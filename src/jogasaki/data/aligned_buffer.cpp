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
#include "aligned_buffer.h"

#include <cstring>
#include <memory>
#include <ostream>

#include <jogasaki/utils/aligned_unique_ptr.h>
#include <jogasaki/utils/binary_printer.h>

namespace jogasaki::data {

aligned_buffer::aligned_buffer(
    std::size_t capacity,
    std::size_t align
) noexcept :
    capacity_(capacity),
    alignment_(align)
{
    std::memset(data_.get(), 0, capacity_);
}

std::size_t aligned_buffer::size() const noexcept {
    return size_;
}

void* aligned_buffer::data() const noexcept {
    return data_.get();
}

aligned_buffer::operator bool() const noexcept {
    return capacity_ != 0;
}

[[nodiscard]] bool aligned_buffer::empty() const noexcept {
    return size_ == 0;
}

void aligned_buffer::resize_internal(std::size_t sz, bool copydata) {
    if(sz <= capacity_) {
        size_ = sz;
        return;
    }
    auto n = utils::make_aligned_array<std::byte>(alignment_, sz);
    std::memset(n.get(), 0, sz);
    if (copydata) {
        std::memcpy(n.get(), data_.get(), size_);
    }
    data_.swap(n);
    capacity_ = sz;
    size_ = sz;
}

void aligned_buffer::resize(std::size_t sz) {
    resize_internal(sz, true);
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
    out << " size: " << value.size()
        << " capacity: " << value.capacity()
        << " alignment: " << value.alignment()
        << " data: " << utils::binary_printer{value.data_.get(), value.size()};
    return out;
}

aligned_buffer& aligned_buffer::assign(aligned_buffer const& other) {
    resize_internal(other.size_, false);
    std::memcpy(data_.get(), other.data_.get(), other.size_);
    return *this;
}

aligned_buffer::operator std::string_view() const noexcept {
    return {reinterpret_cast<char*>(data_.get()), size_};  //NOLINT
}

aligned_buffer::aligned_buffer(std::string_view s) :
    aligned_buffer(s.size())
{
    if (! s.empty()) {
        std::memcpy(data_.get(), s.data(), s.size());
        size_ = s.size();
    }
}

void aligned_buffer::shrink_to_fit() {
    if (size_ == capacity_) {
        return;
    }
    auto n = utils::make_aligned_array<std::byte>(alignment_, size_);
    std::memcpy(n.get(), data_.get(), size_);
    data_.swap(n);
    capacity_ = size_;
}

std::size_t aligned_buffer::capacity() const noexcept {
    return capacity_;
}

aligned_buffer& aligned_buffer::assign(std::string_view sv) {
    return assign(aligned_buffer{sv});
}

void aligned_buffer::dump(std::ostream& out, int indent) const noexcept{
    std::string indent_space(indent, ' ');
    out << indent_space << "aligned_buffer:" << "\n";
    out << indent_space << "  capacity_: " << capacity_ << "\n";
    out << indent_space << "  alignment_: " << alignment_ << "\n";
    out << indent_space << "  size_: " << size_ << "\n";
    out << indent_space << "  data_: " ;
    for (std::size_t i = 0; i < size_; ++i) {
        out << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(data_[i]) << " ";
        if ((i + 1) % 16 == 0) {
            out << std::endl;
        }
    }
    out << std::setfill(' ') << std::dec << std::endl;

}

} // namespace
