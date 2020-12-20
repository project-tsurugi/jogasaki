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
#pragma once

#include <vector>
#include <cstring>
#include <cstddef>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/utils/aligned_unique_ptr.h>
#include <jogasaki/utils/binary_printer.h>

namespace jogasaki::data {

using takatori::util::maybe_shared_ptr;

/**
 * @brief records container to store just handful of records
 */
class aligned_buffer {
public:

    /**
     * @brief create empty object
     */
    aligned_buffer() = default;

    /**
     * @brief create new instance
     * @param meta the record metadata
     * @param varlen_resource memory resource used to store the varlen data referenced from the records stored in this
     * instance. nullptr is allowed if this instance stores only the copy of reference to varlen data (shallow copy.)
     * @param capacity the capacity of the container
     */
    explicit aligned_buffer(
        std::size_t size,
        std::size_t align = 1
    ) :
        capacity_(size),
        alignment_(align),
        data_(utils::make_aligned_array<std::byte>(align, size))
    {}

    /**
     * @brief getter for the number of data count added to this store
     * @return the number of records
     */
    [[nodiscard]] std::size_t size() const noexcept {
        return capacity_;
    }

    /**
     * @brief getter for the number of data count added to this store
     * @return the number of records
     */
    [[nodiscard]] void* data() const noexcept {
        return data_.get();
    }

    /**
     * @brief return whether the object is valid or not
     */
    [[nodiscard]] explicit operator bool() const noexcept {
        return capacity_ != 0;
    }

    /**
     * @brief
     */
    void resize(std::size_t sz) noexcept {
        auto n = utils::make_aligned_array<std::byte>(alignment_, sz);
        data_.swap(n);
        capacity_ = sz;
    }

    /**
     * @brief compare contents of two objects
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(aligned_buffer const& a, aligned_buffer const& b) noexcept {
        return a.data_ == b.data_;
    }
    friend bool operator!=(aligned_buffer const& a, aligned_buffer const& b) noexcept {
        return !(a == b);
    }

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, aligned_buffer const& value) {
        out << " capacity: " << value.size()
            << " data: " << utils::binary_printer{value.data_.get(), value.size()};
        return out;
    }
private:
    std::size_t capacity_{};
    std::size_t alignment_{};
    utils::aligned_array<std::byte> data_ = utils::make_aligned_array<std::byte>(0UL, 0UL);
};

} // namespace
