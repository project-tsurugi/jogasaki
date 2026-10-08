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
#include "global.h"

#include <memory>
#include <mutex>
#include <utility>

#include <yugawara/function/configurable_provider.h>
#include <data_relay_grpc/blob_relay/service.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/function/aggregate_function_repository.h>
#include <jogasaki/executor/function/function_registry.h>
#include <jogasaki/executor/function/incremental/aggregate_function_repository.h>
#include <jogasaki/executor/function/scalar_function_repository.h>
#include <jogasaki/executor/function/table_valued_function_repository.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::global {

using takatori::util::maybe_shared_ptr;

memory::page_pool& page_pool(pool_operation op) {
    static std::unique_ptr<memory::page_pool> pool = std::make_unique<memory::page_pool>();
    switch(op) {
        case pool_operation::get:
            break;
        case pool_operation::reset:
            pool = std::make_unique<memory::page_pool>();
            break;
    }
    return *pool;
}

executor::function::function_registry& function_registry(
    std::shared_ptr<executor::function::function_registry> const& owner) {
    static std::weak_ptr<executor::function::function_registry> active{};
    static std::mutex active_mutex{};
    std::lock_guard<std::mutex> guard{active_mutex};
    if (owner) { active = owner; }
    auto registry = active.lock();
    if (!registry) { fail_with_exception(); }
    return *registry;
}

executor::function::incremental::aggregate_function_repository& incremental_aggregate_function_repository() {
    return function_registry().incremental_functions();
}

executor::function::aggregate_function_repository& aggregate_function_repository() {
    return function_registry().aggregate_functions();
}

executor::function::scalar_function_repository& scalar_function_repository() {
    return function_registry().scalar_functions();
}

executor::function::table_valued_function_repository& table_valued_function_repository() {
    return function_registry().table_functions();
}

std::shared_ptr<yugawara::function::configurable_provider> const&
regular_function_provider(std::shared_ptr<yugawara::function::configurable_provider> arg) {
    auto& registry = function_registry();
    if (arg) { registry.regular_provider(std::move(arg)); }
    return registry.regular_provider();
}

maybe_shared_ptr<configuration> const& config_pool(maybe_shared_ptr<configuration> arg) {
    static maybe_shared_ptr<configuration> pool = std::make_shared<configuration>();
    if(arg) {
        pool = std::move(arg);
    }
    return pool;
}

std::shared_ptr<kvs::database> const& db(std::shared_ptr<kvs::database> arg) {
    static std::shared_ptr<kvs::database> db = std::make_shared<kvs::database>();
    if(arg) {
        db = std::move(arg);
    }
    return db;
}

std::shared_ptr<api::impl::database> const& database_impl(std::shared_ptr<api::impl::database> arg) {
    static std::shared_ptr<api::impl::database> db = nullptr;
    if(arg) {
        db = std::move(arg);
    }
    return db;
}

std::shared_ptr<storage::storage_manager> const& storage_manager(std::shared_ptr<storage::storage_manager> arg) {
    static std::shared_ptr<storage::storage_manager> mgr = std::make_shared<storage::storage_manager>();
    if(arg) {
        mgr = std::move(arg);
    }
    return mgr;
}

static std::shared_ptr<data_relay_grpc::blob_relay::blob_relay_service> const&
relay_service_internal(std::shared_ptr<data_relay_grpc::blob_relay::blob_relay_service> arg, bool set) {
    static std::shared_ptr<data_relay_grpc::blob_relay::blob_relay_service> relay_service = nullptr;
    if(set) {
        relay_service = std::move(arg);
    }
    return relay_service;
}

void relay_service(std::shared_ptr<data_relay_grpc::blob_relay::blob_relay_service> arg) {
    relay_service_internal(std::move(arg), true);
}

std::shared_ptr<data_relay_grpc::blob_relay::blob_relay_service> const& relay_service() {
    return relay_service_internal({}, false);
}

}  // namespace jogasaki::global
