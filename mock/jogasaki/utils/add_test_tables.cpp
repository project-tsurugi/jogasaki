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
#include "add_test_tables.h"

#include <memory>

#include <yugawara/storage/configurable_provider.h>

#include <jogasaki/executor/global.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/tables.h>

namespace jogasaki::utils {

void add_test_tables() {
    auto tables = std::make_shared<yugawara::storage::configurable_provider>();
    utils::add_test_tables(*tables);
    executor::register_kvs_storage(*tables);
    global::database_impl()->reset_tables();
    global::database_impl()->recover_metadata();
}

void add_benchmark_tables() {
    auto tables = std::make_shared<yugawara::storage::configurable_provider>();
    utils::add_benchmark_tables(*tables);
    executor::register_kvs_storage(*tables);
    global::database_impl()->reset_tables();
    global::database_impl()->recover_metadata();
}

}  // namespace jogasaki::testing
