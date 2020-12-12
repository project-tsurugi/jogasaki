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
#include <set>
#include <memory>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/fail.h>
#include <takatori/util/enum_tag.h>

#include <jogasaki/executor/function/aggregate_function_kind.h>
#include <jogasaki/executor/function/aggregator_info.h>

#include "builtin_functions.h"

namespace jogasaki::executor::function {

using takatori::util::sequence_view;
using takatori::util::enum_tag_t;

aggregate_function_info_impl<aggregate_function_kind::sum>::aggregate_function_info_impl() :
    aggregate_function_info(
        kind,
        { aggregator_info{ builtin::sum, 1 } },
        { aggregator_info{ builtin::sum, 1 } },
        { aggregator_info{ builtin::identity_post, 1 } }
    )
{}

aggregate_function_info_impl<aggregate_function_kind::count>::aggregate_function_info_impl() :
    aggregate_function_info(
        kind,
        { aggregator_info{ builtin::count_pre, 1 } },
        { aggregator_info{ builtin::count_mid, 1 } },
        { aggregator_info{ builtin::identity_post, 1 } }
    )
{}

aggregate_function_info_impl<aggregate_function_kind::avg>::aggregate_function_info_impl() :
    aggregate_function_info(
        kind,
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
}
