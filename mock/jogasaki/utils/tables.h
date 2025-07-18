/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <yugawara/storage/configurable_provider.h>

namespace jogasaki::utils {

/**
 * @brief add benchmark table definitions to the provider
 * @param provider object to register the benchmark tables
 */
void add_benchmark_tables(yugawara::storage::configurable_provider& provider);

/**
 * @brief add test table definitions to the provider
 * @param provider object to register the built-in tables
 */
void add_test_tables(yugawara::storage::configurable_provider& provider);

}  // namespace jogasaki::utils
