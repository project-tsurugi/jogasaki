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
#include <jogasaki/udf/plugin_loader.h>
#include <jogasaki/udf/udf_loader.h>
#include <vector>
namespace jogasaki::executor::function {
void add_udf_scalar_functions(::yugawara::function::configurable_provider& functions,
    executor::function::scalar_function_repository& repo,
    const std::vector<std::tuple<std::shared_ptr<plugin::udf::plugin_api>,
        std::shared_ptr<plugin::udf::generic_client>>>& plugins);
} // namespace jogasaki::executor::function
