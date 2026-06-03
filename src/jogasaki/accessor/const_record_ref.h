/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <takatori/util/assertion.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/constants.h>
#include <jogasaki/utils/assert.h>

namespace jogasaki::accessor {

/**
 * @brief read-only record reference providing access to record contents
 * @details A const counterpart of record_ref. Provides read-only accessors for field values
 * and nullity, but does not expose any write operations. Implicitly constructible from record_ref
 * to allow existing callers passing record_ref to upgraded function parameters.
 */
class const_record_ref {
public:

    /// @brief type for record size
    using size_type = std::size_t;

    /// @brief type of value/nullity offset
    using offset_type = std::size_t;

    /**
     * @brief construct "undefined" object representing invalid reference
     */
    constexpr const_record_ref() = default;

    /**
     * @brief construct object from pointer and size
     * @param data base pointer indicating record body (read-only)
     * @param size record size
     */
    constexpr const_record_ref(void const* data, size_type size) noexcept : data_(data), size_(size) {}

    /**
     * @brief implicit conversion from record_ref
     * @details Allows read-write record_ref to be passed wherever const_record_ref is expected.
     * @param r the source record_ref
     */
    constexpr const_record_ref(record_ref r) noexcept : data_(r.data()), size_(r.size()) {}  //NOLINT(google-explicit-constructor,hicpp-explicit-conversions)

    /**
     * @brief retrieve nullity
     * @param nullity_offset nullity bit offset for the field, whose nullity is to be retrieved.
     * @return true if the field is null
     * @return false otherwise
     * @warning this function is meaningful only when the field is nullable.
     * For non-nullable field, the return value should not be used and ignored.
     */
    [[nodiscard]] bool is_null(offset_type nullity_offset) const;

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
        assert_with_exception(value_offset < size_, value_offset, size_);
        static_assert(std::is_trivially_copy_constructible_v<T>);
        T data{};
        std::memcpy(&data, static_cast<char const*>(data_) + value_offset, sizeof(T)); //NOLINT
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
        assert_with_exception(value_offset < size_, value_offset, size_);
        static_assert(std::is_trivially_copy_constructible_v<T>);
        return *reinterpret_cast<T const*>(static_cast<char const*>(data_) + value_offset);  //NOLINT
    }

    /**
     * @brief nullable field value getter
     * @tparam T runtime type of each field
     * @param nullity_offset nullity bit offset of the field whose value will be retrieved
     * @param value_offset offset of the field whose value will be retrieved
     * @return optional containing the field value or empty if it's null
     */
    template<typename T>
    [[nodiscard]] std::optional<T> get_if(offset_type nullity_offset, offset_type value_offset) const {
        assert_with_exception(nullity_offset / bits_per_byte < size_, nullity_offset, size_);
        assert_with_exception(value_offset < size_, value_offset, size_);
        if (is_null(nullity_offset)) {
            return {};
        }
        return get_value<T>(value_offset);
    }

    /**
     * @brief getter for record size
     * @return size of record
     */
    [[nodiscard]] constexpr size_type size() const noexcept {
        return size_;
    }

    /**
     * @brief getter for validity of record reference
     * @return whether this reference is valid or not
     */
    [[nodiscard]] constexpr explicit operator bool() const noexcept {
        return data_ != nullptr;
    }

    /**
     * @brief getter of pointer to record data
     * @return the base pointer for the record data (read-only)
     */
    [[nodiscard]] constexpr void const* data() const noexcept {
        return data_;
    }

private:
    void const* data_{};
    size_type size_{};
};

static_assert(std::is_trivially_copyable_v<const_record_ref>);
static_assert(std::is_trivially_destructible_v<const_record_ref>);

}  // namespace jogasaki::accessor
