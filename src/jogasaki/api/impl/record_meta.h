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

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string_view>
#include <type_traits>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/field_type.h>
#include <jogasaki/api/record_meta.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::api::impl {

using takatori::util::maybe_shared_ptr;

/**
 * @brief record metadata holding information about field types, nullability
 */
class record_meta : public api::record_meta {
public:
    /// @brief field index type (origin = 0)
    using field_index_type = std::size_t;

    /**
     * @brief construct empty object
     */
    constexpr record_meta() = default;

    /**
     * @brief construct new object
     */
    explicit record_meta(maybe_shared_ptr<meta::external_record_meta> meta);

    /**
     * @brief getter for field type - same as operator[] but friendly style for pointers
     * @param index field index
     * @return field type
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] field_type const& at(field_index_type index) const noexcept override;

    /**
     * @brief getter for the nullability for the field
     * @param index field index
     * @return true if the field is nullable
     * @return false otherwise
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] bool nullable(field_index_type index) const noexcept override;

    /**
     * @brief retrieve number of fields in the record
     * @return number of the fields
     */
    [[nodiscard]] std::size_t field_count() const noexcept override;

    /**
     * @brief accessor to the original record metadata
     * @return the metadata
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept;

    /**
     * @brief accessor to the field name
     * @return the field name
     */
    [[nodiscard]] std::optional<std::string_view> field_name(field_index_type index) const noexcept override;

private:
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    std::vector<impl::field_type> fields_{};
};

} // namespace

