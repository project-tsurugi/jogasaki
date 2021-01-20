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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/field_type.h>
#include <jogasaki/api/record_meta.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::api::impl {

using takatori::util::maybe_shared_ptr;

/**
 * @brief record metadata holding information about field types, nullability and binary encoding of records.
 * @details based on the record metadata and knowledge on binary encoding/bits layout, record_meta provides information
 * to access its data via record_ref accessor (e.g. offset for field value or nullity bit.)
 */
class record_meta : public api::record_meta {
public:
    /// @brief fields type
    using fields_type = std::vector<field_type>;

    /// @brief iterator for fields
    using field_iterator = fields_type::const_iterator;

    /// @brief field index type (origin = 0)
    using field_index_type = std::size_t;

    /// @brief the value indicating invalid offset
    constexpr static std::size_t npos = static_cast<std::size_t>(-1);

    /**
     * @brief construct empty object
     */
    constexpr record_meta() = default;

    /**
     * @brief construct new object
     */
    explicit record_meta(maybe_shared_ptr<meta::record_meta> meta) :
        meta_(std::move(meta))
    {
        fields_.reserve(meta_->field_count());
        for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
            fields_.emplace_back(meta_->at(i));
        }
    }

    /**
     * @brief getter for field type - same as operator[] but friendly style for pointers
     * @param index field index
     * @return field type
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] field_type const& at(field_index_type index) const noexcept override {
        return fields_[index];
    }

    /**
     * @brief getter for the nullability for the field
     * @param index field index
     * @return true if the field is nullable
     * @return false otherwise
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] bool nullable(field_index_type index) const noexcept override {
        return meta_->nullable(index);
    }

    /**
     * @brief retrieve number of fields in the record
     * @return number of the fields
     */
    [[nodiscard]] std::size_t field_count() const noexcept override {
        return meta_->field_count();
    }

private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    std::vector<impl::field_type> fields_{};
};

} // namespace

