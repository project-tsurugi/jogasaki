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

#include <memory>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/fail.h>

#include <jogasaki/executor/function/value_generator.h>
#include <jogasaki/executor/function/field_locator.h>
#include <jogasaki/accessor/record_ref.h>

namespace jogasaki::executor::function::incremental {

using takatori::util::sequence_view;

/**
 * @brief definition of aggregator function type
 */
using aggregator_type = std::function<void (
    accessor::record_ref,
    field_locator const&,
    bool,
    accessor::record_ref,
    sequence_view<field_locator const>
)>;

/**
 * @brief aggregator information
 * @details aggregators are the concrete functions composing a aggregate function
 * with a optional value generator for empty input.
 */
class aggregator_info {
public:
    /**
     * @brief create empty object
     */
    aggregator_info() = default;

    ~aggregator_info() = default;
    aggregator_info(aggregator_info const& other) = default;
    aggregator_info& operator=(aggregator_info const& other) = default;
    aggregator_info(aggregator_info&& other) noexcept = default;
    aggregator_info& operator=(aggregator_info&& other) noexcept = default;

    /**
     * @brief create new object
     * @param aggregator the concrete aggregation function
     * @param arg_count the number of arguments for the function
     * @param empty_generator the value generator function to create value for empty aggregation
     * (e.g. zero for COUNT, or NULL for SUM)
     */
    aggregator_info(
        aggregator_type aggregator,
        std::size_t arg_count,
        empty_value_generator_type empty_generator = {}
    );

    /**
     * @brief accessor to aggregator
     */
    [[nodiscard]] aggregator_type const& aggregator() const noexcept;

    /**
     * @brief return whether the info contains valid aggregator or not
     */
    [[nodiscard]] explicit operator bool() const noexcept;

    /**
     * @brief return the number of args that the aggregator function accepts
     */
    [[nodiscard]] std::size_t arg_count() const noexcept;

    /**
     * @brief accessor to empty value generator
     */
    [[nodiscard]] empty_value_generator_type const& empty_value_generator() const noexcept;

private:
    bool valid_{false};
    aggregator_type aggregator_{};
    std::size_t arg_count_{};
    empty_value_generator_type empty_generator_{};
};

}
