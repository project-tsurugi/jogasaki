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
#include <algorithm>
#include <type_traits>
#include <boost/dynamic_bitset.hpp>

#include <meta/field_type.h>
#include <constants.h>

namespace dc::meta {

/**
 * @brief record metadata holding information about field types, nullability and binary encoding of records.
 * @details based on the record metadata and knowledge on binary encoding/bits layout, record_meta provides information
 * to access its data via record_ref accessor (e.g. offset for field value or nullity bit.)
 */
class record_meta final {
public:
    /// @brief entity type
    using value_entity_type = std::vector<field_type>;

    /// @brief byte offset of field value
    using value_offset_type = std::size_t;

    /// @brief bit offset of field nullity
    using nullity_offset_type = std::size_t;

    /// @brief field index type (origin = 0)
    using field_index_type = std::size_t;

    /// @brief the bitset entity type for nullability
    using nullability_entity_type = boost::dynamic_bitset<std::uint64_t>;

    /// @brief the value offset table type
    using value_offset_table_type = std::vector<value_offset_type>;

    /// @brief the nullity offset table type
    using nullity_offset_table_type = std::vector<nullity_offset_type>;

    /// @brief the value indicating invalid offset
    constexpr static std::size_t npos = static_cast<std::size_t>(-1);

    /// @brief max alignment required for record buffer
    constexpr static std::size_t max_alignment = std::max({
//            field_type_traits<field_type_kind::undefined>::alignment,
            field_type_traits<field_type_kind::boolean>::alignment,
            field_type_traits<field_type_kind::int1>::alignment,
            field_type_traits<field_type_kind::int2>::alignment,
            field_type_traits<field_type_kind::int4>::alignment,
            field_type_traits<field_type_kind::int8>::alignment,
            field_type_traits<field_type_kind::float4>::alignment,
            field_type_traits<field_type_kind::float8>::alignment,
            field_type_traits<field_type_kind::decimal>::alignment,
            field_type_traits<field_type_kind::character>::alignment,
//            field_type_traits<field_type_kind::bit>::alignment,
            field_type_traits<field_type_kind::date>::alignment,
            field_type_traits<field_type_kind::time_of_day>::alignment,
            field_type_traits<field_type_kind::time_point>::alignment,
//            field_type_traits<field_type_kind::time_interval>::alignment,
//            field_type_traits<field_type_kind::array>::alignment,
//            field_type_traits<field_type_kind::record>::alignment,
//            field_type_traits<field_type_kind::unknown>::alignment,
//            field_type_traits<field_type_kind::row_reference>::alignment,,
//            field_type_traits<field_type_kind::row_id>::alignment,
//            field_type_traits<field_type_kind::declared>::alignment,
//            field_type_traits<field_type_kind::extension>::alignment,
    });
    /**
     * @brief construct empty object
     */
    constexpr record_meta() = default;

    /**
     * @brief construct new object
     * @param entity ordered list of field types
     * @param nullability ordered list of nullability bits, whose size must be equal to number of fields in the record
     * @param value_offset_table ordered list of value offset, whose size must be equal to number of fields in the record
     * @param nullity_offset_table ordered list of nullity offset, whose size must be equal to number of fields in the record
     */
    record_meta(value_entity_type entity,
            nullability_entity_type nullability,
            value_offset_table_type value_offset_table,
            nullity_offset_table_type nullity_offset_table,
            std::size_t record_alignment,
            std::size_t record_size) :
            entity_(std::move(entity)), nullability_(std::move(nullability)), field_count_(entity_.size()),
            value_offset_table_(std::move(value_offset_table)), nullity_offset_table_(std::move(nullity_offset_table)),
            record_alignment_(record_alignment), record_size_(record_size) {
        assert(field_count_ == nullability_.size()); // NOLINT
        assert(field_count_ == value_offset_table_.size()); // NOLINT
        assert(field_count_ == nullity_offset_table_.size()); // NOLINT
    }

    /**
     * @brief construct new object
     * @param entity ordered list of field types
     * @param nullability ordered list of nullability bits, whose size must be equal to number of fields in the record
     */
    record_meta(value_entity_type entity, nullability_entity_type nullability) :
            entity_(std::move(entity)), nullability_(std::move(nullability)), field_count_(entity_.size()) {
        assert(field_count_ == nullability_.size()); // NOLINT
        calculate_default_layout_offset();
    }

    /**
     * @brief getter for field type
     * @param index field index
     * @return field type
     * @warning if index is not in valid range, the behavior is undefined
     */
    field_type const& operator[](field_index_type index) const noexcept {
        return entity_[index];
    }

    /**
     * @brief getter for field type - same as operator[] but friendly style for pointers
     * @param index field index
     * @return field type
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] field_type const& at(field_index_type index) const noexcept {
        return entity_[index];
    }

    /**
     * @brief getter for byte offset for field value
     * @param index field index
     * @return byte offset of the field value, which can be used through accessor api (e.g. set_value/get_value of record_ref)
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] value_offset_type value_offset(field_index_type index) const noexcept {
        return value_offset_table_[index];
    }

    /**
     * @brief getter for the nullity bit offset for the field
     * @param index field index
     * @return nullity offset, which can be used through accessor api (e.g. set_null/is_null of record_ref)
     * @warning the return value is valid only when the field specified by the index is nullable
     */
    [[nodiscard]] nullity_offset_type nullity_offset(field_index_type index) const noexcept {
        return nullity_offset_table_[index];
    }

    /**
     * @brief getter for the nullability for the field
     * @param index field index
     * @return true if the field is nullable
     * @return false otherwise
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] bool nullable(field_index_type index) const noexcept {
        return nullability_[index];
    }

    /**
     * @brief retrieve alignment of the record
     * @return alignment of the record
     */
    [[nodiscard]] std::size_t record_alignment() const noexcept {
        return record_alignment_;
    }

    /**
     * @brief retrieve size of the record
     * @return size of the record
     */
    [[nodiscard]] std::size_t record_size() const noexcept {
        return record_size_;
    }

    /**
     * @brief retrieve number of fields in the record
     * @return number of the fields
     */
    [[nodiscard]] std::size_t field_count() const noexcept {
        return field_count_;
    }

private:
    value_entity_type entity_{};
    nullability_entity_type nullability_{};
    std::size_t field_count_{};
    value_offset_table_type value_offset_table_{};
    nullity_offset_table_type nullity_offset_table_{};
    std::size_t record_alignment_{};
    std::size_t record_size_{};

    /**
     * Records are binary encoded as follows: First, the field values encoded by the native format of its runtime type are ordered
     * respecting alignment of each runtime type. (The field order has been given to record_meta via constructor.) Then,
     * nullity bits field follows, whose alignment is 1 byte and its size is the ceiling of number of fields divided by 8(bits_per_byte).
     */
    void calculate_default_layout_offset() {
        std::size_t cur = 0;
        std::size_t record_max_align = 1;
        for(std::size_t i = 0; i < field_count_; ++i) {
            auto&& field = entity_[i];
            auto alignment = field.runtime_type_alignment();
            record_max_align = std::max(record_max_align, alignment);
            cur = (cur + alignment - 1) / alignment * alignment;
            value_offset_table_.emplace_back(cur);
            cur += field.runtime_type_size();
        }
        std::size_t nullity_offset = cur * bits_per_byte;
        for(std::size_t i = 0; i < field_count_; ++i) {
            std::size_t pos = npos;
            if (nullability_[i]) {
                pos = nullity_offset;
                ++nullity_offset;
            }
            nullity_offset_table_.emplace_back(pos);
        }
        cur += (nullability_.count() + bits_per_byte - 1) / bits_per_byte;
        record_alignment_ = record_max_align;
        record_size_ = (cur + record_alignment_ - 1) / record_alignment_ * record_alignment_;
        assert(record_max_align <= max_alignment); //NOLINT
        assert(max_alignment % record_max_align == 0); //NOLINT
    }
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

