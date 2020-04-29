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
#include <cassert>
#include <memory>
#include <cstddef>
#include <cstring>
#include <string_view>
#include <memory/paged_memory_resource.h>

namespace jogasaki::accessor {

/**
 * @brief text field data object
 * Trivially copyable immutable class holding variable length text string (possibly in short format, aks SSO)
 * The memory to store the text string may be allocated from the paged_memory_resource, so life-time of this object
 * should be cared in pair with the paged_memory_resource. When the paged_memory_resource ends lifetime and its resource
 * is released, this object's memory area will also become invalid.
 */
class text {
public:
    using size_type = std::size_t;

    /**
     * @brief default constructor representing text string with 0 length
     */
    constexpr text() noexcept : s_(nullptr) {} //NOLINT

    /**
     * @brief construct new object allocating from the given memory resource when long format is needed
     * @param resource memory resource used to allocate storage for long format
     * @param data pointer to the data area that is copied into tne new object
     * @param size size of the data area
     */
    text(memory::paged_memory_resource* resource, char const* data, size_type size) { //NOLINT
        if (size <= short_text::max_size) {
            s_ = short_text(data, size);  //NOLINT(cppcoreguidelines-pro-type-union-access)
            return;
        }
        auto p = resource->allocate(size, 1);
        std::memcpy(p, data, size);
        l_ = long_text(static_cast<char*>(p), size);  //NOLINT(cppcoreguidelines-pro-type-union-access)
    }

    /**
     * @brief construct new object allocating from the given memory resource when long format is needed
     * @param resource memory resource used to allocate storage for long format
     * @param str text string data copied into the new object
     */
    text(memory::paged_memory_resource* resource, std::string_view str) : text(resource, str.data(), str.size()) {}

    /**
     * @brief implicit conversion to string_view
     */
    explicit operator std::string_view() const noexcept {
        if (is_short()) {
            return {s_.data(), s_.size()};  //NOLINT(cppcoreguidelines-pro-type-union-access)
        }
        return {l_.data(), l_.size()};  //NOLINT(cppcoreguidelines-pro-type-union-access)
    }

    /**
     * @brief return whether the instance is in short format
     */
    [[nodiscard]] bool is_short() const noexcept {
        return s_.is_short();  //NOLINT(cppcoreguidelines-pro-type-union-access)
    }

    /**
     * @brief compare contents of two text object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return negative if a < b
     * @return zero if a = b
     * @return positive if a > b
     */
    friend int compare(text const& a, text const& b) noexcept {
        std::string_view sv_a{a};
        std::string_view sv_b{b};
        return sv_a.compare(sv_b);
    }

    friend bool operator<(text const& a, text const& b) noexcept {
        return compare(a, b) < 0;
    }

    friend bool operator>(text const& a, text const& b) noexcept {
        return compare(a, b) > 0;
    }

    friend bool operator<=(text const& a, text const& b) noexcept {
        return compare(a, b) <= 0;
    }

    friend bool operator>=(text const& a, text const& b) noexcept {
        return compare(a, b) >= 0;
    }

    friend bool operator==(text const& a, text const& b) noexcept {
        std::string_view sv_a{a};
        std::string_view sv_b{b};
        return sv_a == sv_b;
    }

    friend bool operator!=(text const& a, text const& b) noexcept {
        return !(a == b);
    }
private:
    class long_text {
    public:
        static constexpr size_type max_size = ~(size_type { 1 } << (sizeof(size_type) * 8 - 1));
        static constexpr size_type size_mask = max_size;

        long_text() = default;
        long_text(char* allocated_data, size_type size) noexcept
                : data_(allocated_data)
                , size_(size & max_size)
        {
            assert(size <= max_size); // NOLINT
        }

        [[nodiscard]] char const* data() const noexcept {
            return data_;
        }

        [[nodiscard]] size_type size() const noexcept {
            return size_;
        }

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
        explicit constexpr short_text(std::nullptr_t) noexcept : data_(), size_and_is_short_(is_short_mask) {}
        short_text(char const* data, short_size_type size) noexcept // NOLINT
                : size_and_is_short_(size | is_short_mask)
        {
            assert(size <= max_size); // NOLINT
            std::memcpy(&data_[0], data, size & size_mask);
        }

        [[nodiscard]] bool is_short() const noexcept {
            return (size_and_is_short_ & is_short_mask) != 0;
        }

        [[nodiscard]] char const* data() const noexcept {
            return &data_[0];
        }

        [[nodiscard]] size_type size() const noexcept {
            return size_and_is_short_ & size_mask;
        }

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
