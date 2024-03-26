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

#include <memory>
#include <set>
#include <vector>

#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>

#include <jogasaki/executor/function/incremental/aggregate_function_kind.h>
#include <jogasaki/executor/function/incremental/aggregator_info.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::executor::function::incremental {

using takatori::util::sequence_view;
using takatori::util::fail;

/**
 * @brief aggregate function information interface
 */
class aggregate_function_info {
public:
    using aggregators_info = std::vector<aggregator_info>;

    aggregate_function_info() = default;
    virtual ~aggregate_function_info() = default;
    aggregate_function_info(aggregate_function_info const& other) = default;
    aggregate_function_info& operator=(aggregate_function_info const& other) = default;
    aggregate_function_info(aggregate_function_info&& other) noexcept = default;
    aggregate_function_info& operator=(aggregate_function_info&& other) noexcept = default;

    /**
     * @brief create new object
     * @param kind kind of the aggregate function
     * @param pre aggregators used for pre aggregation (input record to values) The empty_generator function must be
     * provided for the pre aggregator_info in order to calculate aggregation with empty input.
     * @param mid aggregators used for intermediate aggregation (merge values)
     * @param post aggregators used for post aggregation (calculate final results from intermediate values)
     */
    aggregate_function_info(
        aggregate_function_kind kind,
        aggregators_info&& pre,
        aggregators_info&& mid,
        aggregators_info&& post
    );

    /**
     * @brief accessor to aggregate function kind
     * @return the kind of the aggregate function
     */
    [[nodiscard]] constexpr aggregate_function_kind kind() const noexcept {
        return kind_;
    }

    /**
     * @brief accessor to pre aggregators
     */
    [[nodiscard]] sequence_view<aggregator_info const> pre() const noexcept;;

    /**
     * @brief accessor to mid aggregators
     */
    [[nodiscard]] sequence_view<aggregator_info const> mid() const noexcept;;

    /**
     * @brief accessor to post aggregators
     */
    [[nodiscard]] sequence_view<aggregator_info const> post() const noexcept;;

    /**
     * @brief fetch field type list used for aggregation calculation
     * @details some aggregate function requires separating fields, calculates incrementally, and gather them (e.g.
     * avg is calcuated by sum and count.) This function returns types for those fields given the input argument types.
     * @param args the types used for the input arguments of this aggregate function
     * @return the list of calculation field types corresponding to the input args
     */
    [[nodiscard]] virtual std::vector<meta::field_type> intermediate_types(
        sequence_view<meta::field_type const> args
    ) const = 0;

private:
    aggregate_function_kind kind_{};
    aggregators_info pre_{};
    aggregators_info mid_{};
    aggregators_info post_{};
};

/**
 * @brief primary template for aggregate function info implementation
 * @tparam Kind
 */
template <aggregate_function_kind Kind>
class aggregate_function_info_impl;

template <>
class aggregate_function_info_impl<aggregate_function_kind::sum> : public aggregate_function_info {
public:
    aggregate_function_info_impl();
    [[nodiscard]] std::vector<meta::field_type> intermediate_types(
        sequence_view<meta::field_type const> args
    ) const override;
};

template <>
class aggregate_function_info_impl<aggregate_function_kind::count> : public aggregate_function_info {
public:
    aggregate_function_info_impl();
    [[nodiscard]] std::vector<meta::field_type> intermediate_types(
        sequence_view<meta::field_type const> args
    ) const override;
};

template <>
class aggregate_function_info_impl<aggregate_function_kind::count_rows> : public aggregate_function_info {
public:
    aggregate_function_info_impl();
    [[nodiscard]] std::vector<meta::field_type> intermediate_types(
        sequence_view<meta::field_type const> args
    ) const override;
};

template <>
class aggregate_function_info_impl<aggregate_function_kind::avg> : public aggregate_function_info {
public:
    aggregate_function_info_impl();
    [[nodiscard]] std::vector<meta::field_type> intermediate_types(
        sequence_view<meta::field_type const> args
    ) const override;
};

template <>
class aggregate_function_info_impl<aggregate_function_kind::max> : public aggregate_function_info {
public:
    aggregate_function_info_impl();
    [[nodiscard]] std::vector<meta::field_type> intermediate_types(
        sequence_view<meta::field_type const> args
    ) const override;
};

template <>
class aggregate_function_info_impl<aggregate_function_kind::min> : public aggregate_function_info {
public:
    aggregate_function_info_impl();
    [[nodiscard]] std::vector<meta::field_type> intermediate_types(
        sequence_view<meta::field_type const> args
    ) const override;
};
}
