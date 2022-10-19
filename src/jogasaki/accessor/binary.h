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

#include "text.h"

namespace jogasaki::accessor {

/**
 * @brief binary field data object
 * @details Trivially copyable immutable class holding variable length binary sequence (possibly in short format, aks SSO)
 * The memory to store the binary may be allocated from the paged_memory_resource, so life-time of this object
 * should be cared in pair with the paged_memory_resource. When the paged_memory_resource ends lifetime and its resource
 * is released, this object's memory area will also become invalid.
 */
class binary {  //NOLINT(cppcoreguidelines-pro-type-union-access)
public:
    /// @brief size of binary data in byte
    using size_type = std::size_t;

    /**
     * @brief default constructor representing binary string with 0 length
     */
    constexpr binary() = default;

    /**
     * @brief construct new object allocating from the given memory resource when long format is needed
     * @param resource memory resource used to allocate storage for long format
     * @param data pointer to the data area that is copied into tne new object
     * @param size size of the data area
     */
    binary(memory::paged_memory_resource* resource, char const* data, size_type size) :
        entity_(resource, data, size)
    {}

    /**
     * @brief construct new object allocating from the given memory resource when long format is needed
     * @param resource memory resource used to allocate storage for long format
     * @param str binary string data copied into the new object
     */
    binary(memory::paged_memory_resource* resource, std::string_view str) :
        entity_(resource, str)
    {}

    /**
     * @brief construct new object allocating from the given memory resource when long format is needed
     * @param resource memory resource used to allocate storage for long format
     * @param src binary string data copied into the new object
     */
    binary(memory::paged_memory_resource* resource, binary src) :
        entity_(resource, src.entity_)
    {}

    /**
     * @brief concatenate two binary data and construct new object allocating from the given memory resource
     * when long format is needed
     * @param resource memory resource used to allocate storage for long format
     * @param src1 first argument for concatenating binary strings copied into the new object
     * @param src2 second argument for concatenating binary strings copied into the new object
     */
    binary(memory::paged_memory_resource* resource, binary src1, binary src2) :
        entity_(resource, src1.entity_, src2.entity_)
    {}

    /**
     * @brief construct new object by directly transferring the data area without copying the data
     * @details this can be used to create binary body beforehand in the memory resource provided area and
     * associate it with new binary object. If the size is small enough, the newly created object becomes short object.
     * @param data the binary data area which is allocated by the memory resource associated with the new object
     * @param size the size of the data area
     * @attention differently from other constructors receiving paged_memory_resource, this constructor
     * doesn't receive one, but the resource allocating `data` area is implicitly associated with this object,
     * and the area pointing by `data` should be kept as long as this object is actively used.
     */
    binary(char const* data, binary::size_type size) :
        entity_(data, size)
    {}


    /**
     * @brief construct new object by directly transferring the data area without copying the data
     * @details this can be used to create binary body beforehand in the memory resource provided area and
     * associate it with new binary object. If the size is small enough, the newly created object becomes short object.
     * @param str the binary data area which is allocated by the memory resource associated with the new object
     * @attention differently from other constructors receiving paged_memory_resource, this constructor
     * doesn't receive one, but the resource allocating `data` area is implicitly associated with this object,
     * and the area pointing by `data` should be kept as long as this object is actively used.
     */
    explicit binary(std::string_view str) :
        entity_(str)
    {}

    /**
     * @brief construct new object using string literal
     * @details this can be used to create binary object associated with the literal data.
     * If the size is small enough, the newly created object becomes short object.
     * @param data the binary data area which is allocated by the memory resource associated with the new object
     * @attention differently from other constructors receiving paged_memory_resource, this constructor doesn't
     * receive one and is not associated with memory resource. This object simply points to pre-allocated
     * area for literal data.
     */
    template<std::size_t N>
    explicit binary(const char (&data)[N]) : binary(const_cast<char*>(data), N-1) {} //NOLINT

    /**
     * @brief implicit conversion to string_view with lvalue
     * @note casting to string_view works only with lvalue because string_view can reference
     * SSO'ed data stored in the accessor::binary.
     */
    [[nodiscard]] explicit operator std::string_view() const & noexcept {
        return static_cast<std::string_view>(entity_);
    }

    /**
     * @brief deleting implicit conversion to string_view with rvalue
     * @note this should not be used because string_view can reference SSO'ed data stored in the accessor::binary.
     */
    [[nodiscard]] explicit operator std::string_view() && noexcept = delete;

    /**
     * @brief implicit conversion to string
     * @warning this copies string data and is not very performant compared to casting to string_view
     */
    [[nodiscard]] explicit operator std::string() const noexcept {
        return static_cast<std::string>(entity_);
    }

    /**
     * @brief return whether the instance is in short format
     */
    [[nodiscard]] bool is_short() const noexcept {
        return entity_.is_short();
    }

    /**
     * @brief returns whether the content is empty or not
     */
    [[nodiscard]] bool empty() const noexcept {
        return entity_.empty();
    }

    /**
     * @brief returns whether the content is non-empty
     */
    [[nodiscard]] explicit operator bool() const noexcept {
        return static_cast<bool>(entity_);
    }

    /**
     * @brief returns the size of the content binary string
     */
    [[nodiscard]] std::size_t size() const noexcept {
        return entity_.size();
    }

    /**
     * @brief compare contents of two binary object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return negative if a < b
     * @return zero if a = b
     * @return positive if a > b
     */
    friend int compare(binary const& a, binary const& b) noexcept {
        return compare(a.entity_, b.entity_);
    }

    /**
     * @brief compare contents of two binary object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a < b
     * @return false otherwise
     */
    friend bool operator<(binary const& a, binary const& b) noexcept {
        return a.entity_ < b.entity_;
    }

    /**
     * @brief compare contents of two binary object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a > b
     * @return false otherwise
     */
    friend bool operator>(binary const& a, binary const& b) noexcept {
        return a.entity_ > b.entity_;
    }

    /**
     * @brief compare contents of two binary object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a <= b
     * @return false otherwise
     */
    friend bool operator<=(binary const& a, binary const& b) noexcept {
        return a.entity_ <= b.entity_;
    }

    /**
     * @brief compare contents of two binary object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a >= b
     * @return false otherwise
     */
    friend bool operator>=(binary const& a, binary const& b) noexcept {
        return a.entity_ >= b.entity_;
    }

    /**
     * @brief compare contents of two binary object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(binary const& a, binary const& b) noexcept {
        return a.entity_ == b.entity_;
    }

    /**
     * @brief compare contents of two binary object lexicographically
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(binary const& a, binary const& b) noexcept {
        return a.entity_ != b.entity_;
    }

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, binary const& value) {
        return out << value.entity_;
    }

private:
    accessor::text entity_{};
};

static_assert(std::is_trivially_copyable_v<binary>);
static_assert(std::is_trivially_destructible_v<binary>);
static_assert(std::alignment_of_v<binary> == 8);
static_assert(sizeof(binary) == 16);

} // namespace jogasaki::accessor

/**
 * @brief std::hash specialization for binary
 */
template<>
struct std::hash<jogasaki::accessor::binary> {
    /**
     * @brief compute hash of the given object.
     * @param value the target object
     * @return computed hash code
     */
    std::size_t operator()(jogasaki::accessor::binary const& value) const noexcept {
        return std::hash<std::string_view>{}(static_cast<std::string_view>(value));
    }
};

