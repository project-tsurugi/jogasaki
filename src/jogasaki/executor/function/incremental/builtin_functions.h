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

#include <takatori/util/sequence_view.h>
#include <yugawara/aggregate/configurable_provider.h>

#include <jogasaki/executor/function/incremental/field_locator.h>
#include <jogasaki/executor/function/incremental/aggregate_function_repository.h>
#include <jogasaki/accessor/record_ref.h>

namespace jogasaki::executor::function::incremental {

/**
 * @brief register built-in aggregate functions to the given provider and function repository
 * @param functions the provider where the built-in functions are registered
 * @param repo the function repository where the built-in functions are registered
 */
void add_builtin_aggregate_functions(
    ::yugawara::aggregate::configurable_provider& functions,
    executor::function::incremental::aggregate_function_repository& repo
);

namespace builtin {

using takatori::util::sequence_view;

void sum(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args
);

void count_pre(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args
);

void count_mid(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args
);

void avg_post(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args
);

void identity_post(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args
);
} // namespace builtin

}
