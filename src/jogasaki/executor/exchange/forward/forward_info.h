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
#include <memory>
#include <optional>
#include <set>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/executor/partitioner.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor::exchange::forward {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;

/**
 * @brief information to execute forward, used to extract schema and record layout information for key/value parts
 */
class forward_info {
public:
    using field_index_type = meta::record_meta::field_index_type;

    /**
     * @brief construct empty object
     */
    forward_info() = default;

    /**
     * @brief construct new object
     * @param meta the metadata of the input record for forward operation
     * @param limit the record limit set for the forward operation
     * @param respect_input whether the row count is applied in the order of the input relation
     * @param preserve_output whether the output keeps the order of the input relation
     */
    explicit forward_info(
        maybe_shared_ptr<meta::record_meta> meta,
        std::optional<std::size_t> limit = {},
        bool respect_input = false,
        bool preserve_output = false
    );

    /**
     * @brief returns metadata for whole record
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& record_meta() const noexcept;

    /**
     * @brief returns limit of number of records to forward
     */
    [[nodiscard]] std::optional<std::size_t> limit() const noexcept;

    /**
     * @brief returns whether the row count is applied in the order of the input relation
     * @details the input relation must be an ordered relation, i.e. the upstream process is not split
     * (takatori::plan::forward_ordering::respect_input)
     */
    [[nodiscard]] bool respect_input() const noexcept;

    /**
     * @brief returns whether the output keeps the order of the input relation
     * @details the downstream process must not be split (takatori::plan::forward_ordering::preserve_output)
     */
    [[nodiscard]] bool preserve_output() const noexcept;

private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    std::optional<std::size_t> limit_{};
    bool respect_input_{};
    bool preserve_output_{};
};

}  // namespace jogasaki::executor::exchange::forward
