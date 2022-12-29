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

#include <vector>
#include <set>
#include <memory>
#include <optional>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>

#include <jogasaki/constants.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/executor/partitioner.h>
#include <jogasaki/executor/comparator.h>

namespace jogasaki::executor::exchange::group {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;

/**
 * @brief information to execute shuffle, used to extract schema and record layout information for key/value parts
 */
class group_info {
public:
    using field_index_type = meta::record_meta::field_index_type;

    /**
     * @brief construct empty object
     */
    group_info() = default;

    /**
     * @brief construct new object
     * @param record the metadata of the input record for shuffle operation
     * @param key_indices the ordered indices to choose the grouping key fields from the record
     * @param key_indices_for_sort the ordered indices to specify additional key fields to sort group members
     * within groups
     * @param key_ordering_for_sort info to specify key fields' ordering spec to sort group members
     * @param limit the record limit per group
     */
    group_info(
        maybe_shared_ptr<meta::record_meta> record,
        std::vector<field_index_type> key_indices,
        std::vector<field_index_type> const& key_indices_for_sort = {},
        std::vector<ordering> const& key_ordering_for_sort = {},
        std::optional<std::size_t> limit = {}
    );

    /**
     * @brief extract key part from the input record
     */
    [[nodiscard]] accessor::record_ref extract_key(accessor::record_ref record) const noexcept;

    /**
     * @brief extract sort key (grouping key fields + fields for member sorting) part from the input record
     */
    [[nodiscard]] accessor::record_ref extract_sort_key(accessor::record_ref record) const noexcept;

    /**
     * @brief extract value part (fields outside grouping key) from the input record
     */
    [[nodiscard]] accessor::record_ref extract_value(accessor::record_ref record) const noexcept;

    /**
     * @brief returns metadata for whole record
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& record_meta() const noexcept;

    /**
     * @brief returns metadata for grouping key part
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& key_meta() const noexcept;

    /**
     * @brief return compare info to compare keys
     */
    [[nodiscard]] class compare_info const& compare_info() const noexcept;

    /**
     * @brief returns metadata for sort key (grouping key fields + fields for member sorting) part
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& sort_key_meta() const noexcept;

    /**
     * @brief returns sort key ordering
     */
    [[nodiscard]] sequence_view<ordering const> sort_key_ordering() const noexcept;

    /**
     * @brief return compare info to compare sort keys
     */
    [[nodiscard]] class compare_info const& sort_compare_info() const noexcept;

    /**
     * @brief returns metadata for value part (fields outside grouping key)
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& value_meta() const noexcept;

    /**
     * @brief returns metadata for grouping key/value parts at once
     */
    [[nodiscard]] maybe_shared_ptr<meta::group_meta> const& group_meta() const noexcept;

    /**
     * @brief returns limit of number of records per group
     */
    [[nodiscard]] std::optional<std::size_t> const& limit() const noexcept;
private:
    maybe_shared_ptr<meta::record_meta> record_{};
    std::vector<field_index_type> key_indices_{};
    maybe_shared_ptr<meta::group_meta> group_{};
    maybe_shared_ptr<meta::record_meta> sort_key_{};
    std::vector<ordering> sort_key_ordering_{};
    std::optional<std::size_t> limit_{};
    class compare_info compare_info_{};
    class compare_info sort_compare_info_{};


    [[nodiscard]] std::shared_ptr<meta::record_meta> from_keys(
        maybe_shared_ptr<meta::record_meta> const& record,
        std::vector<std::size_t> const& indices
    );
    [[nodiscard]] std::shared_ptr<meta::record_meta> create_value_meta(
        maybe_shared_ptr<meta::record_meta> const& record,
        std::vector<std::size_t> const& key_indices
    );
    [[nodiscard]] std::shared_ptr<meta::record_meta> create_sort_key_meta(
        maybe_shared_ptr<meta::record_meta> const& record,
        std::vector<std::size_t> const& indices,
        std::vector<std::size_t> const& sort_key_indices
    );
    [[nodiscard]] std::vector<ordering>  create_sort_key_ordering(
        std::size_t group_key_count,
        std::vector<ordering> const& sort_key_ordering
    );
};

}
