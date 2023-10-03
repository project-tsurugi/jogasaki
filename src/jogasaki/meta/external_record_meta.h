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
#pragma once

#include <cstddef>
#include <algorithm>
#include <type_traits>

#include <boost/dynamic_bitset.hpp>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/constants.h>

namespace jogasaki::meta {

using takatori::util::maybe_shared_ptr;

/**
 * @brief record metadata holding information about field types, nullability and binary encoding of records.
 * @details based on the record metadata and knowledge on binary encoding/bits layout, record_meta provides information
 * to access its data via record_ref accessor (e.g. offset for field value or nullity bit.)
 */
class external_record_meta final {
public:
    /// @brief fields type
    using fields_type = record_meta::fields_type;

    /// @brief iterator for fields
    using field_iterator = fields_type::const_iterator;

    /// @brief byte offset of field value
    using value_offset_type = record_meta::value_offset_type;

    /// @brief bit offset of field nullity
    using nullity_offset_type = record_meta::nullity_offset_type;

    /// @brief field index type (origin = 0)
    using field_index_type = record_meta::field_index_type;

    /// @brief the bitset type for nullability
    using nullability_type = record_meta::nullability_type;

    /// @brief the value offset table type
    using value_offset_table_type = record_meta::value_offset_table_type;

    /// @brief the nullity offset table type
    using nullity_offset_table_type = record_meta::nullity_offset_table_type;

    /// @brief the constant to indicate field index is not defined
    constexpr static field_index_type undefined = static_cast<field_index_type>(-1);

    /**
     * @brief construct empty object
     */
    constexpr external_record_meta() = default;

    /**
     * @brief construct new object with default layout defined by field types and nullability
     */
    external_record_meta(
        maybe_shared_ptr<record_meta> origin,
        std::vector<std::optional<std::string>> field_names
    );

    /**
     * @brief getter for field type
     * @param index field index
     * @return field type
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] field_type const& operator[](field_index_type index) const noexcept;

    /**
     * @brief getter for field type - same as operator[] but friendly style for pointers
     * @param index field index
     * @return field type
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] field_type const& at(field_index_type index) const noexcept;

    /**
     * @brief getter for byte offset for field value
     * @param index field index
     * @return byte offset of the field value, which can be used through accessor api
     * (e.g. set_value/get_value of record_ref)
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] value_offset_type value_offset(field_index_type index) const noexcept;

    /**
     * @brief getter for the nullity bit offset for the field
     * @param index field index
     * @return nullity offset, which can be used through accessor api (e.g. set_null/is_null of record_ref)
     * @warning the return value is valid only when the field specified by the index is nullable
     */
    [[nodiscard]] nullity_offset_type nullity_offset(field_index_type index) const noexcept;

    /**
     * @brief getter for the nullability for the field
     * @param index field index
     * @return true if the field is nullable
     * @return false otherwise
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] bool nullable(field_index_type index) const noexcept;

    /**
     * @brief retrieve alignment of the record
     * @return alignment of the record
     */
    [[nodiscard]] std::size_t record_alignment() const noexcept;

    /**
     * @brief retrieve size of the record
     * @return size of the record
     */
    [[nodiscard]] std::size_t record_size() const noexcept;

    /**
     * @brief retrieve number of fields in the record
     * @return number of the fields
     */
    [[nodiscard]] std::size_t field_count() const noexcept;

    /**
     * @brief begin field iterator
     * @return field iterator at the beginning
     */
    [[nodiscard]] field_iterator begin() const noexcept;

    /**
     * @brief end field iterator
     * @return field iterator at the end
     */
    [[nodiscard]] field_iterator end() const noexcept;

    /**
     * @brief accessor to field name
     * @param index the field index to fetch name
     * @return the field name, empty if it's not defined
     */
    [[nodiscard]] std::optional<std::string_view> field_name(field_index_type index) const noexcept;

    /**
     * @brief find field index by field name
     * @param name the field name to search
     * @return the field index
     * @return undefined if not found
     */
    [[nodiscard]] field_index_type field_index(std::string_view name) const noexcept;
    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output stream
     */
    friend inline std::ostream& operator<<(std::ostream& out, external_record_meta const& value) {
        for(std::size_t i=0, n=value.field_count(); i < n; ++i) {
            auto& name = value.field_names_[i];
            out << (name ? "\""+ *name + "\":" : "");
            out << value[i];
            if (value.nullable(i)) {
                out << "*";
            }
            out << "[" << value.value_offset(i);
            if (value.nullable(i)) {
                out << ", " << value.nullity_offset(i);
            }
            out << "] ";
        }
        return out;
    }

    /**
     * @brief accessor to the original meta::record_meta object
     * @return the original record meta
     */
    [[nodiscard]] maybe_shared_ptr<record_meta> const& origin() noexcept;

private:
    maybe_shared_ptr<record_meta> record_meta_{};
    std::vector<std::optional<std::string>> field_names_{};
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(external_record_meta const& a, external_record_meta const& b) noexcept {
    if (a.field_count() != b.field_count()) {
        return false;
    }
    for(std::size_t i=0; i < a.field_count(); ++i) {
        if (a[i] != b[i]) {
            return false;
        }
        if (a.nullable(i) != b.nullable(i)) {
            return false;
        }
    }
    return true;
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(external_record_meta const& a, external_record_meta const& b) noexcept {
    return !(a == b);
}

} // namespace

