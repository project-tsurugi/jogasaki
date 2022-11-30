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

#include <yugawara/storage/configurable_provider.h>
#include <jogasaki/kvs/database.h>

namespace jogasaki::executor {

/**
 * @brief add built-in table definitions to the provider
 * @param provider object to register the built-in tables
 */
void add_builtin_tables(yugawara::storage::configurable_provider& provider);

/**
 * @brief add benchmark table definitions to the provider
 * @param provider object to register the benchmark tables
 */
void add_benchmark_tables(yugawara::storage::configurable_provider& provider);

/**
 * @brief add analytics benchmark table definitions to the provider
 * @param provider object to register the benchmark tables
 */
void add_analytics_benchmark_tables(yugawara::storage::configurable_provider& provider);

/**
 * @brief add test table definitions to the provider
 * @param provider object to register the built-in tables
 */
void add_test_tables(yugawara::storage::configurable_provider& provider);

/**
 * @brief add QA test table definitions to the provider
 * @param provider object to register the built-in tables
 */
void add_qa_tables(yugawara::storage::configurable_provider& provider);

/**
 * @brief add phone-billing benchmark table definitions to the provider
 * @param provider object to register the built-in tables
 */
void add_phone_bill_tables(yugawara::storage::configurable_provider& provider);

/**
 * @brief create kvs storage based on the index definitions in the provider
 * @param db the database where the kvs storage will be created
 * @param provider object to provide index definition
 */
void register_kvs_storage(kvs::database& db, yugawara::storage::configurable_provider& provider);

}
