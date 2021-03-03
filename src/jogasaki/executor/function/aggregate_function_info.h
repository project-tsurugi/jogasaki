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
#include <takatori/util/sequence_view.h>
#include <takatori/util/enum_tag.h>
#include <takatori/util/fail.h>

#include <jogasaki/data/value_store.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/function/aggregate_function_kind.h>
#include <jogasaki/executor/function/field_locator.h>

namespace jogasaki::executor::function {

using takatori::util::sequence_view;
using takatori::util::enum_tag;
using takatori::util::fail;

/**
 * @brief definition of aggregator function type
 */
using aggregator_type = std::function<void (
    accessor::record_ref,
    field_locator const&,
    sequence_view<std::reference_wrapper<data::value_store> const>
)>;

/**
 * @brief aggregate function information interface
 */
class aggregate_function_info {
public:
    aggregate_function_info() = default;
    ~aggregate_function_info() = default;
    aggregate_function_info(aggregate_function_info const& other) = default;
    aggregate_function_info& operator=(aggregate_function_info const& other) = default;
    aggregate_function_info(aggregate_function_info&& other) noexcept = default;
    aggregate_function_info& operator=(aggregate_function_info&& other) noexcept = default;

    /**
     * @brief create new object
     * @param kind kind of the aggregate function
     */
    aggregate_function_info(
        aggregate_function_kind kind,
        aggregator_type aggregator,
        std::size_t arg_count = 1
    ) :
        kind_(kind),
        aggregator_(std::move(aggregator)),
        arg_count_(arg_count)
    {}

    /**
     * @brief accessor to aggregate function kind
     * @return the kind of the aggregate function
     */
    [[nodiscard]] constexpr aggregate_function_kind kind() const noexcept {
        return kind_;
    }

    /**
     * @brief accessor to aggregate function
     */
    [[nodiscard]] aggregator_type const& aggregator() const noexcept {
        return aggregator_;
    }

    /**
     * @brief accessor to arg count
     */
    [[nodiscard]] std::size_t arg_count() const noexcept {
        return arg_count_;
    }

private:
    aggregate_function_kind kind_{};
    aggregator_type aggregator_{};
    std::size_t arg_count_{};
};

}
