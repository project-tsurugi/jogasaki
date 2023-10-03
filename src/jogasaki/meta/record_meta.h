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

#include <jogasaki/meta/field_type.h>
#include <jogasaki/constants.h>

namespace jogasaki::meta {

/**
 * @brief record metadata holding information about field types, nullability and binary encoding of records.
 * @details based on the record metadata and knowledge on binary encoding/bits layout, record_meta provides information
 * to access its data via record_ref accessor (e.g. offset for field value or nullity bit.)
 */
class record_meta final {
public:
    /// @brief fields type
    using fields_type = std::vector<field_type>;

    /// @brief iterator for fields
    using field_iterator = fields_type::const_iterator;

    /// @brief byte offset of field value
    using value_offset_type = std::size_t;

    /// @brief bit offset of field nullity
    using nullity_offset_type = std::size_t;

    /// @brief field index type (origin = 0)
    using field_index_type = std::size_t;

    /// @brief the bitset type for nullability
    using nullability_type = boost::dynamic_bitset<std::uint64_t>;

    /// @brief the value offset table type
    using value_offset_table_type = std::vector<value_offset_type>;

    /// @brief the nullity offset table type
    using nullity_offset_table_type = std::vector<nullity_offset_type>;

    /// @brief the value indicating invalid offset
    constexpr static std::size_t npos = static_cast<std::size_t>(-1);

    /// @brief max alignment required for record buffer
    constexpr static std::size_t max_alignment = std::max({  //NOLINT(cert-err58-cpp)
//        field_type_traits<field_type_kind::undefined>::alignment,
        field_type_traits<field_type_kind::boolean>::alignment,
        field_type_traits<field_type_kind::int1>::alignment,
        field_type_traits<field_type_kind::int2>::alignment,
        field_type_traits<field_type_kind::int4>::alignment,
        field_type_traits<field_type_kind::int8>::alignment,
        field_type_traits<field_type_kind::float4>::alignment,
        field_type_traits<field_type_kind::float8>::alignment,
        field_type_traits<field_type_kind::decimal>::alignment,
        field_type_traits<field_type_kind::character>::alignment,
//        field_type_traits<field_type_kind::bit>::alignment,
        field_type_traits<field_type_kind::date>::alignment,
        field_type_traits<field_type_kind::time_of_day>::alignment,
        field_type_traits<field_type_kind::time_point>::alignment,
//        field_type_traits<field_type_kind::time_interval>::alignment,
//        field_type_traits<field_type_kind::array>::alignment,
//        field_type_traits<field_type_kind::record>::alignment,
//        field_type_traits<field_type_kind::unknown>::alignment,
//        field_type_traits<field_type_kind::row_reference>::alignment,
//        field_type_traits<field_type_kind::row_id>::alignment,
//        field_type_traits<field_type_kind::declared>::alignment,
//        field_type_traits<field_type_kind::extension>::alignment,
        field_type_traits<field_type_kind::pointer>::alignment,
    });
    /**
     * @brief construct empty object
     */
    constexpr record_meta() = default;

    /**
     * @brief construct new object
     * @param fields ordered list of field types
     * @param nullability ordered list of nullability bits, whose size must be equal to
     * number of fields in the record
     * @param value_offset_table ordered list of value offset, whose size must be equal to
     * number of fields in the record
     * @param nullity_offset_table ordered list of nullity offset, whose size must be equal to
     * number of fields in the record
     * @param record_alignment the alignment of the record
     * @param record_size the size of the record in byte
     */
    record_meta(fields_type fields,
            nullability_type nullability,
            value_offset_table_type value_offset_table,
            nullity_offset_table_type nullity_offset_table,
            std::size_t record_alignment,
            std::size_t record_size);

    /**
     * @brief construct new object with default layout defined by field types and nullability
     * @param fields ordered list of field types
     * @param nullability ordered list of nullability bits, whose size must be equal to number of fields in the record
     * @param record_size the size of the record in byte. This can be used to customize the record size. If npos is
     * specified, the record size is calculated from the fields and nullability. The record_size must be equal to
     * or greater than the calculated length.
     */
    record_meta(
        fields_type fields,
        nullability_type nullability,
        std::size_t record_size = npos
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
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output stream
     */
    friend inline std::ostream& operator<<(std::ostream& out, record_meta const& value) {
        for(std::size_t i=0, n=value.field_count(); i < n; ++i) {
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

private:
    fields_type fields_{};
    nullability_type nullability_{};
    std::size_t field_count_{};
    value_offset_table_type value_offset_table_{};
    nullity_offset_table_type nullity_offset_table_{};
    std::size_t record_alignment_{1UL};
    std::size_t record_size_{};
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(record_meta const& a, record_meta const& b) noexcept {
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
inline bool operator!=(record_meta const& a, record_meta const& b) noexcept {
    return !(a == b);
}

} // namespace

