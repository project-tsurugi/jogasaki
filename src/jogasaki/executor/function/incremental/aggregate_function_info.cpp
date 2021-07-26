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
#include "aggregate_function_info.h"

#include <vector>

#include <takatori/util/sequence_view.h>

#include <jogasaki/executor/function/incremental/aggregate_function_kind.h>
#include <jogasaki/executor/function/incremental/aggregator_info.h>

#include "builtin_functions.h"

namespace jogasaki::executor::function::incremental {

using takatori::util::sequence_view;

aggregate_function_info_impl<aggregate_function_kind::sum>::aggregate_function_info_impl() :
    aggregate_function_info(
        aggregate_function_kind::sum,
        { aggregator_info{ builtin::sum, 1, null_generator } },
        { aggregator_info{ builtin::sum, 1 } },
        { aggregator_info{ builtin::identity_post, 1 } }
    )
{}

std::vector<meta::field_type> aggregate_function_info_impl<aggregate_function_kind::sum>::intermediate_types(
    sequence_view<const meta::field_type> args) const {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    return {args.begin(), args.end()};
}

aggregate_function_info_impl<aggregate_function_kind::count>::aggregate_function_info_impl() :
    aggregate_function_info(
        aggregate_function_kind::count,
        { aggregator_info{ builtin::count_pre, 1 , zero_generator } },
        { aggregator_info{ builtin::count_mid, 1 } },
        { aggregator_info{ builtin::identity_post, 1 } }
    )
{}

std::vector<meta::field_type>
aggregate_function_info_impl<aggregate_function_kind::count>::intermediate_types(
    sequence_view<const meta::field_type>) const {
    return {meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>}};
}

aggregate_function_info_impl<aggregate_function_kind::count_rows>::aggregate_function_info_impl() :
    aggregate_function_info(
        aggregate_function_kind::count_rows,
        { aggregator_info{ builtin::count_rows_pre, 0, zero_generator } },
        { aggregator_info{ builtin::count_mid, 1 } },
        { aggregator_info{ builtin::identity_post, 1 } }
    )
{}

std::vector<meta::field_type>
aggregate_function_info_impl<aggregate_function_kind::count_rows>::intermediate_types(
    sequence_view<const meta::field_type>) const {
    return {meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>}};
}

aggregate_function_info_impl<aggregate_function_kind::avg>::aggregate_function_info_impl() :
    aggregate_function_info(
        aggregate_function_kind::avg,
        {
            aggregator_info{ builtin::sum, 1, null_generator },
            aggregator_info{ builtin::count_pre, 1, null_generator },
        },
        {
            aggregator_info{ builtin::sum, 1 },
            aggregator_info{ builtin::count_mid, 1 },
        },
        { aggregator_info{ builtin::avg_post, 2 } }
    )
{}

std::vector<meta::field_type> aggregate_function_info_impl<aggregate_function_kind::avg>::intermediate_types(
    sequence_view<const meta::field_type> args) const {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    return {
        args[0],
        meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>}
    };
}

aggregate_function_info_impl<aggregate_function_kind::max>::aggregate_function_info_impl() :
    aggregate_function_info(
        aggregate_function_kind::max,
        { aggregator_info{ builtin::max, 1, null_generator } },
        { aggregator_info{ builtin::max, 1 } },
        { aggregator_info{ builtin::identity_post, 1 } }
    )
{}

std::vector<meta::field_type> aggregate_function_info_impl<aggregate_function_kind::max>::intermediate_types(
    sequence_view<const meta::field_type> args) const {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    return {args.begin(), args.end()};
}

aggregate_function_info_impl<aggregate_function_kind::min>::aggregate_function_info_impl() :
    aggregate_function_info(
        aggregate_function_kind::min,
        { aggregator_info{ builtin::min, 1, null_generator } },
        { aggregator_info{ builtin::min, 1 } },
        { aggregator_info{ builtin::identity_post, 1 } }
    )
{}

std::vector<meta::field_type> aggregate_function_info_impl<aggregate_function_kind::min>::intermediate_types(
    sequence_view<const meta::field_type> args) const {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    return {args.begin(), args.end()};
}

aggregate_function_info::aggregate_function_info(
    aggregate_function_kind kind,
    aggregate_function_info::aggregators_info&& pre,
    aggregate_function_info::aggregators_info&& mid,
    aggregate_function_info::aggregators_info&& post
) :
    kind_(kind),
    pre_(std::move(pre)),
    mid_(std::move(mid)),
    post_(std::move(post))
{
    for(auto&& info : pre_) {
        (void)info;
        BOOST_ASSERT(info.empty_value_generator());  //NOLINT
    }
}

sequence_view<aggregator_info const> aggregate_function_info::pre() const noexcept {
    return pre_;
}

sequence_view<aggregator_info const> aggregate_function_info::mid() const noexcept {
    return mid_;
}

sequence_view<aggregator_info const> aggregate_function_info::post() const noexcept {
    return post_;
}

}
