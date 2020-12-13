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

#include <jogasaki/executor/function/aggregate_function_kind.h>
#include <jogasaki/executor/function/aggregator_info.h>

#include "builtin_functions.h"

namespace jogasaki::executor::function {

using takatori::util::sequence_view;

aggregate_function_info_impl<aggregate_function_kind::sum>::aggregate_function_info_impl() :
    aggregate_function_info(
        aggregate_function_kind::sum,
        { aggregator_info{ builtin::sum, 1 } },
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
        { aggregator_info{ builtin::count_pre, 1 } },
        { aggregator_info{ builtin::count_mid, 1 } },
        { aggregator_info{ builtin::identity_post, 1 } }
    )
{}

std::vector<meta::field_type>
aggregate_function_info_impl<aggregate_function_kind::count>::intermediate_types(
    sequence_view<const meta::field_type>) const {
    return {meta::field_type{enum_tag<meta::field_type_kind::int8>}};
}

aggregate_function_info_impl<aggregate_function_kind::avg>::aggregate_function_info_impl() :
    aggregate_function_info(
        aggregate_function_kind::avg,
        {
            aggregator_info{ builtin::sum, 1 },
            aggregator_info{ builtin::count_pre, 1 },
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
        meta::field_type{enum_tag<meta::field_type_kind::int8>}
    };
}

}
