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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/constants.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/executor/partitioner.h>
#include <jogasaki/executor/comparator.h>

namespace jogasaki::executor::exchange::group {

using takatori::util::maybe_shared_ptr;

/**
 * @brief information to execute shuffle, used to extract schema and record layout information for key/value parts
 */
class shuffle_info {
public:
    using field_index_type = meta::record_meta::field_index_type;

    /**
     * @brief construct empty object
     */
    shuffle_info() = default;

    /**
     * @brief construct new object
     * @param record the metadata of the input record for shuffle operation
     * @param key_indices the ordered indices to choose the keys from the record fields
     */
    shuffle_info(maybe_shared_ptr<meta::record_meta> record, std::vector<field_index_type> key_indices);

    /**
     * @brief extract key part from the input record
     */
    [[nodiscard]] accessor::record_ref extract_key(accessor::record_ref record) const noexcept;

    /**
     * @brief extract value part from the input record
     */
    [[nodiscard]] accessor::record_ref extract_value(accessor::record_ref record) const noexcept;

    /**
     * @brief returns metadata for whole record
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& record_meta() const noexcept;

    /**
     * @brief returns metadata for key part
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& key_meta() const noexcept;

    /**
     * @brief returns metadata for value part
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& value_meta() const noexcept;

    /**
     * @brief returns metadata for key/value parts at once
     */
    [[nodiscard]] maybe_shared_ptr<meta::group_meta> const& group_meta() const noexcept;

private:
    maybe_shared_ptr<meta::record_meta> record_{};
    std::vector<field_index_type> key_indices_{};
    maybe_shared_ptr<meta::group_meta> group_{};

    [[nodiscard]] std::shared_ptr<meta::record_meta> create_meta(std::vector<std::size_t> const& indices);
    [[nodiscard]] std::shared_ptr<meta::record_meta> create_key_meta() {
        return create_meta(key_indices_);
    }
    [[nodiscard]] std::shared_ptr<meta::record_meta> create_value_meta();
};

}
