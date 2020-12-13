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

#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/executor/function/aggregate_function_kind.h>
#include <jogasaki/executor/function/aggregator_info.h>

namespace jogasaki::executor::function {

using takatori::util::sequence_view;
using takatori::util::enum_tag;
using takatori::util::fail;

class aggregate_function_info {
public:
    using aggregators_info = std::vector<aggregator_info>;

    virtual ~aggregate_function_info() = default;
    aggregate_function_info(aggregate_function_info const& other) = default;
    aggregate_function_info& operator=(aggregate_function_info const& other) = default;
    aggregate_function_info(aggregate_function_info&& other) noexcept = default;
    aggregate_function_info& operator=(aggregate_function_info&& other) noexcept = default;

    aggregate_function_info(
        aggregate_function_kind kind,
        aggregators_info&& pre,
        aggregators_info&& mid,
        aggregators_info&& post
    ) :
        kind_(kind),
        pre_(std::move(pre)),
        mid_(std::move(mid)),
        post_(std::move(post))
    {}

    [[nodiscard]] constexpr aggregate_function_kind kind() const noexcept {
        return kind_;
    }

    [[nodiscard]] sequence_view<aggregator_info const> pre() const noexcept { return pre_; };
    [[nodiscard]] sequence_view<aggregator_info const> mid() const noexcept { return mid_; };
    [[nodiscard]] sequence_view<aggregator_info const> post() const noexcept { return post_; };

    [[nodiscard]] virtual sequence_view<meta::field_type const> internal_field_types(sequence_view<meta::field_type const> arg_types) const = 0;

private:
    aggregate_function_kind kind_;
    aggregators_info pre_{};
    aggregators_info mid_{};
    aggregators_info post_{};
};

template <aggregate_function_kind Kind>
class aggregate_function_info_impl;

template <>
class aggregate_function_info_impl<aggregate_function_kind::sum> : public aggregate_function_info {
public:
    constexpr static aggregate_function_kind function_kind = aggregate_function_kind::sum;
    aggregate_function_info_impl();
    [[nodiscard]] sequence_view<meta::field_type const> internal_field_types(sequence_view<meta::field_type const> arg_types) const override;
};

template <>
class aggregate_function_info_impl<aggregate_function_kind::count> : public aggregate_function_info {
public:
    constexpr static aggregate_function_kind function_kind = aggregate_function_kind::count;
    aggregate_function_info_impl();
    [[nodiscard]] sequence_view<meta::field_type const> internal_field_types(sequence_view<meta::field_type const>) const override;
private:
    std::vector<meta::field_type> field_types_{meta::field_type{enum_tag<meta::field_type_kind::int8>}};
};

template <>
class aggregate_function_info_impl<aggregate_function_kind::avg> : public aggregate_function_info {
public:
    constexpr static aggregate_function_kind function_kind = aggregate_function_kind::avg;
    aggregate_function_info_impl();
    [[nodiscard]] sequence_view<meta::field_type const> internal_field_types(sequence_view<meta::field_type const> arg_types) const override;
private:
    std::vector<meta::field_type> field_types_int4_{
        meta::field_type{enum_tag<meta::field_type_kind::int4>},
        meta::field_type{enum_tag<meta::field_type_kind::int8>},
    };
    std::vector<meta::field_type> field_types_int8_{
        meta::field_type{enum_tag<meta::field_type_kind::int8>},
        meta::field_type{enum_tag<meta::field_type_kind::int8>},
    };
    std::vector<meta::field_type> field_types_float4_{
        meta::field_type{enum_tag<meta::field_type_kind::float4>},
        meta::field_type{enum_tag<meta::field_type_kind::int8>},
    };
    std::vector<meta::field_type> field_types_float8_{
        meta::field_type{enum_tag<meta::field_type_kind::float8>},
        meta::field_type{enum_tag<meta::field_type_kind::int8>},
    };
};

}
