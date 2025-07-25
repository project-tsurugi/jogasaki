/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <cstring>
#include <optional>
#include <type_traits>
#include <boost/assert.hpp>

#include <takatori/util/assertion.h>

#include <jogasaki/constants.h>

namespace jogasaki::accessor {

/**
 * @brief record reference providing access to record contents
 * @details Given underlying record represented by continuous memory region, this class gives setter/getter
 * for field values or other data manipulating functions. It's assumed that a part of record metadata is
 * shared outside of this class. For example, caller/callee share the offset (for value and nullity) and runtime
 * C++ type used for each field in the record and use them with setter/getter.
 */
class record_ref {
public:

    /// @brief type for record size
    using size_type = std::size_t;

    /// @brief type of value/nullity offset
    using offset_type = std::size_t;

    /**
     * @brief construct "undefined" object representing invalid reference
     */
    constexpr record_ref() = default;

    /**
     * @brief construct object from pointer and size
     * @param data base pointer indicating record body
     * @param size record size
     */
    record_ref(void* data, size_type size) noexcept : data_(data), size_(size) {}

    /**
     * @brief retrieve nullity
     * @param nullity_offset nullity bit offset for the field, whose nullity is to be retrieved.
     * @return true if the field is null
     * @return false otherwise
     * @warning this function is meaningful only when the field is nullable.
     * For non-nullable field, the return value should not be used and ignored.
     */
    [[nodiscard]] bool is_null(offset_type nullity_offset) const noexcept;

    /**
     * @brief set nullity
     * @param nullity_offset nullity bit offset for the field, whose nullity is to be set.
     * @param nullity whether the field is null, or not
     * @warning this function is meaningful only when the field is nullable.
     * For non-nullable field, the nullity should be governed outside and field-level nullity should not be set.
     * @warning for nullable field, nullity has priority to existence of value, that is, even if the value is already
     * set beforehand, and this function sets nullity = true, then the value is ignored and the field
     * is handled as null.
     */
    void set_null(offset_type nullity_offset, bool nullity = true) noexcept;

    /**
     * @brief field value setter
     * @tparam T runtime type of the field whose value will be set
     * @param value_offset byte offset of the field whose value will be set
     * @param x new value for the field
     * @warning this function doesn't change nullity of the field.
     * So if the field is nullable, call set_null() to mark it as non-null.
     */
    template<typename T>
    void set_value(offset_type value_offset, T x) {
        BOOST_ASSERT(value_offset < size_);  //NOLINT
        static_assert(std::is_trivially_copy_constructible_v<T>);
        std::memcpy( static_cast<char*>(data_) + value_offset, &x, sizeof(T)); //NOLINT
    }

    /**
     * @brief field value getter
     * @tparam T runtime type of each field
     * @param value_offset byte offset of the field whose value will be retrieved
     * @warning for nullable field, caller is responsible for checking nullity (e.g. by calling is_null())
     * to validate the return value.
     * If nullity is true for nullable field, returned value by this function should be ignored and the field
     * should be handled as null.
     */
    template<typename T>
    [[nodiscard]] T get_value(offset_type value_offset) const {
        BOOST_ASSERT(value_offset < size_);  //NOLINT
        static_assert(std::is_trivially_copy_constructible_v<T>);
        T data{};
        std::memcpy(&data, static_cast<char*>(data_) + value_offset, sizeof(T)); //NOLINT
        return data;
    }

    /**
     * @brief field value reference getter
     * @tparam T runtime type of each field
     * @param value_offset byte offset of the field whose value will be retrieved
     * @warning for nullable field, caller is responsible for checking nullity (e.g. by calling is_null())
     * to validate the return value.
     * If nullity is true for nullable field, returned value by this function should be ignored and the field
     * should be handled as null.
     */
    template<typename T>
    [[nodiscard]] T const& get_reference(offset_type value_offset) const {
        BOOST_ASSERT(value_offset < size_);  //NOLINT
        static_assert(std::is_trivially_copy_constructible_v<T>);
        return *reinterpret_cast<T const*>(static_cast<char*>(data_) + value_offset);  //NOLINT
    }

    /**
     * @brief nullable field value getter
     * @tparam T runtime type of each field
     * @param nullity_offset nullity bit offset of the field whose value will be retrieved
     * @param value_offset offset of the field whose value will be retrieved
     * @return optional containing the field value or nullptr if it's null
     */
    template<typename T>
    [[nodiscard]] std::optional<T> get_if(offset_type nullity_offset, offset_type value_offset) const {
        BOOST_ASSERT(nullity_offset / bits_per_byte < size_);  //NOLINT
        BOOST_ASSERT(value_offset < size_);  //NOLINT
        if(is_null(nullity_offset)) {
            return {};
        }
        return get_value<T>(value_offset);
    }

    /**
     * @brief getter for record size
     * @return size of record
     */
    [[nodiscard]] size_type size() const noexcept;

    /**
     * @brief getter for validity of record reference
     * @return whether this reference is valid or not
     */
    explicit operator bool() const noexcept;

    /**
     * @brief getter of pointer to record data
     * @return the base pointer for the record data
     */
    [[nodiscard]] void* data() const noexcept;

private:
    void* data_{};
    size_type size_{};
};

static_assert(std::is_trivially_copyable_v<record_ref>);
static_assert(std::is_trivially_destructible_v<record_ref>);

}
