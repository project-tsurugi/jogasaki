/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <yugawara/function/configurable_provider.h>

#include <jogasaki/executor/function/table_valued_function_repository.h>

namespace jogasaki::executor::function {

/**
 * @brief adds the built-in table-valued function declarations to the function provider.
 * @param functions the function provider to register table-valued function signatures
 * @param repo the repository to add table-valued function execution info
 */
void add_builtin_table_valued_functions(
    yugawara::function::configurable_provider& functions,
    table_valued_function_repository& repo
);

}  // namespace jogasaki::executor::function
