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

#include <cstddef>
#include <string_view>

#include <jogasaki/utils/aligned_unique_ptr.h>

namespace jogasaki::data {

/**
 * @brief small buffer to keep aligned data
 */
class aligned_buffer {
public:

    constexpr static std::size_t default_alignment = 1;

    /**
     * @brief create default object - alignment = 1 with no capacity
     * @details the default size is zero and it is expected to be used after resize()
     */
    aligned_buffer() = default;

    /**
     * @brief create new instance
     * @param size the size(capacity) of the buffer
     * @param align the alignment of the buffer
     */
    explicit aligned_buffer(
        std::size_t size,
        std::size_t align = default_alignment
    ) noexcept;

    /**
     * @brief create buffer for given string view
     * @details the buffer has capacity equals to the string length with default alignment.
     */
    explicit aligned_buffer(std::string_view s);

    /**
     * @brief copy constructor
     */
    aligned_buffer(aligned_buffer const& other);

    /**
     * @brief copy assign
     */
    aligned_buffer& operator=(aligned_buffer const& other);

    /**
     * @brief destruct the object
     */
    ~aligned_buffer() = default;

    /**
     * @brief move constructor
     */
    aligned_buffer(aligned_buffer&& other) noexcept = default;

    /**
     * @brief move assign
     */
    aligned_buffer& operator=(aligned_buffer&& other) noexcept = default;

    /**
     * @brief accessor as string_view
     */
    [[nodiscard]] operator std::string_view() const noexcept;  //NOLINT

    /**
     * @brief getter for the capacity of the buffer
     */
    [[nodiscard]] std::size_t size() const noexcept;

    /**
     * @brief getter for the buffer pointer
     * @warning the returned pointer becomes invalid when this object is modified
     * by non-const member functions (e.g. resize()).
     */
    [[nodiscard]] void* data() const noexcept;

    /**
     * @brief return whether the object has capacity larger than zero
     */
    [[nodiscard]] explicit operator bool() const noexcept;

    /**
     * @brief return whether the object has capacity larger than zero
     */
    [[nodiscard]] bool empty() const noexcept;

    /**
     * @brief relocate the buffer for different capacity
     * @param sz the new buffer capacity
     * @details the new buffer is allocated and old one will be released. The alignment is not changed.
     */
    void resize(std::size_t sz);

    /**
     * @brief return alignment of the buffer
     */
    [[nodiscard]] std::size_t alignment() const noexcept;

    /**
     * @brief compare two objects
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(aligned_buffer const& a, aligned_buffer const& b) noexcept;
    friend bool operator!=(aligned_buffer const& a, aligned_buffer const& b) noexcept;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, aligned_buffer const& value);

private:
    std::size_t capacity_{};
    std::size_t alignment_{default_alignment};
    utils::aligned_array<std::byte> data_ = utils::make_aligned_array<std::byte>(alignment_, capacity_);
};

} // namespace
