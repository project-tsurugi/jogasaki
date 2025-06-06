/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <functional>

#include <takatori/util/sequence_view.h>
#include <yugawara/function/configurable_provider.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/value_store.h>
#include <jogasaki/executor/function/scalar_function_repository.h>

namespace jogasaki::executor::function {

/**
 * @brief register built-in scalar functions to the given provider and function repository
 * @param functions the provider where the built-in functions are registered
 * @param repo the function repository where the built-in functions are registered
 */
void add_builtin_scalar_functions(
    ::yugawara::function::configurable_provider& functions,
    executor::function::scalar_function_repository& repo
);

namespace builtin {

using takatori::util::sequence_view;

data::any octet_length(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any current_date(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any localtime(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any current_timestamp(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any localtimestamp(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any substring(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any upper(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any lower(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any character_length(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any abs(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any position(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any mod(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any ceil(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any floor(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any round(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

}  // namespace builtin

}  // namespace jogasaki::executor::function
