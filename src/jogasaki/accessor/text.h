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

#include <limits>
#include <memory>
#include <cstddef>
#include <cstring>
#include <string_view>

#include <jogasaki/memory/paged_memory_resource.h>

namespace jogasaki::accessor {

/**
 * @brief text field data object
 * @details Trivially copyable immutable class holding variable length text string (possibly in short format, aks SSO)
 * The memory to store the text string may be allocated from the paged_memory_resource, so life-time of this object
 * should be cared in pair with the paged_memory_resource. When the paged_memory_resource ends lifetime and its resource
 * is released, this object's memory area will also become invalid.
 */
class text {
public:
    /// @brief size of text data in byte
    using size_type = std::size_t;

    /**
     * @brief default constructor representing text string with 0 length
     */
    constexpr text() noexcept: s_(nullptr) {}  //NOLINT

    /**
     * @brief construct new object allocating from the given memory resource when long format is needed
     * @param resource memory resource used to allocate storage for long format
     * @param data pointer to the data area that is copied into tne new object
     * @param size size of the data area
     */
    text(memory::paged_memory_resource* resource, char const* data, size_type size);

    /**
     * @brief construct new object allocating from the given memory resource when long format is needed
     * @param resource memory resource used to allocate storage for long format
     * @param str text string data copied into the new object
     */
    text(memory::paged_memory_resource* resource, std::string_view str);

    /**
     * @brief implicit conversion to string_view
     */
    explicit operator std::string_view() const noexcept;

    /**
     * @brief return whether the instance is in short format
     */
    [[nodiscard]] bool is_short() const noexcept;

    /**
     * @brief compare contents of two text object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return negative if a < b
     * @return zero if a = b
     * @return positive if a > b
     */
    friend int compare(text const& a, text const& b) noexcept;

    /**
     * @brief compare contents of two text object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a < b
     * @return false otherwise
     */
    friend bool operator<(text const& a, text const& b) noexcept;

    /**
     * @brief compare contents of two text object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a > b
     * @return false otherwise
     */
    friend bool operator>(text const& a, text const& b) noexcept;

    /**
     * @brief compare contents of two text object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a <= b
     * @return false otherwise
     */
    friend bool operator<=(text const& a, text const& b) noexcept;

    /**
     * @brief compare contents of two text object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a >= b
     * @return false otherwise
     */
    friend bool operator>=(text const& a, text const& b) noexcept;

    /**
     * @brief compare contents of two text object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(text const& a, text const& b) noexcept;

    /**
     * @brief compare contents of two text object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(text const& a, text const& b) noexcept;

private:
    class long_text {
    public:
        static constexpr size_type max_size = ~(size_type { 1 } << (sizeof(size_type) * 8 - 1));
        static constexpr size_type size_mask = max_size;

        long_text() = default;
        long_text(char* allocated_data, size_type size) noexcept;

        [[nodiscard]] char const* data() const noexcept;
        [[nodiscard]] size_type size() const noexcept;
    private:
        char* data_;
        size_type size_;
    };

    class short_text {
    public:
        static_assert((sizeof(long_text) & (sizeof(long_text) - 1)) == 0,
                "must be size of long_text is power of 2");

        using short_size_type = unsigned char;
        static constexpr short_size_type is_short_mask = 0x80U;
        static constexpr short_size_type size_mask = 0x7f;
        static constexpr size_type max_size = (sizeof(long_text) - 1) & 0x7f; // NOLINT

        short_text() = default;
        explicit constexpr short_text(std::nullptr_t) noexcept: data_(), size_and_is_short_(is_short_mask) {}
        short_text(char const* data, short_size_type size) noexcept;

        [[nodiscard]] bool is_short() const noexcept;
        [[nodiscard]] char const* data() const noexcept;
        [[nodiscard]] size_type size() const noexcept;
    private:
        char data_[max_size]; // NOLINT
        short_size_type size_and_is_short_;
        // NOTE: assumes little endian
    };

    union {
        long_text l_;
        short_text s_;
    };
};

static_assert(std::is_trivially_copyable_v<text>);
static_assert(std::is_trivially_destructible_v<text>);
static_assert(std::alignment_of_v<text> == 8);
static_assert(sizeof(text) == 16);

}

/**
 * @brief std::hash specialization for text
 */
template<>
struct std::hash<jogasaki::accessor::text> {
    /**
     * @brief compute hash of the given object.
     * @param value the target object
     * @return computed hash code
     */
    std::size_t operator()(jogasaki::accessor::text const& value) const noexcept {
        return std::hash<std::string_view>{}(static_cast<std::string_view>(value));
    }
};

