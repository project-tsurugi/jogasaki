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

#include <cstdint>
#include <memory>
#include <shared_mutex>

#include <takatori/util/maybe_shared_ptr.h>

// attention: making globals depend on lower domain slows down compile time
namespace yugawara::function {
template <class Mutex>
class basic_configurable_provider;

using configurable_provider = basic_configurable_provider<std::shared_mutex>;
}

namespace jogasaki::executor::function::incremental {
class aggregate_function_repository;
}

namespace jogasaki::executor::function {
class aggregate_function_repository;
}

namespace jogasaki::executor::function {
class scalar_function_repository;
}

namespace jogasaki::kvs {
class database;
}

namespace jogasaki::api::impl {
class database;
}

namespace jogasaki::memory {
class page_pool;
}

namespace jogasaki {
class configuration;
}

namespace jogasaki::storage {
class storage_manager;
}

namespace data_relay_grpc::blob_relay {
class blob_relay_service;
}

namespace jogasaki::global {

/**
 * @brief operations for the global paged memory pool
 */
enum class pool_operation : std::int32_t {
    /**
     * @brief get global paged memory resource pool
     */
    get = 0,

    /**
     * @brief release current global paged memory resource pool and reset to new one
     */
    reset,
};

/**
 * @brief thread-safe accessor to the global page pool
 * @details the pool will be initialized on the first call and can be shared by multiple threads
 * @param op operation on the page pool
 * @return reference to the pool
 */
[[nodiscard]] memory::page_pool& page_pool(pool_operation op = pool_operation::get);

/**
 * @brief thread-safe accessor to the global repository for incremental aggregate functions
 * @details the repository will be initialized on the first call and can be shared by multiple threads
 * @return reference to the repository
 */
[[nodiscard]] executor::function::incremental::aggregate_function_repository& incremental_aggregate_function_repository();

/**
 * @brief thread-safe accessor to the global repository for aggregate functions
 * @details the repository will be initialized on the first call and can be shared by multiple threads
 * @return reference to the repository
 */
[[nodiscard]] executor::function::aggregate_function_repository& aggregate_function_repository();

/**
 * @brief thread-safe accessor to the global repository for aggregate functions
 * @details the repository will be initialized on the first call and can be shared by multiple threads
 * @return reference to the repository
 */
[[nodiscard]] executor::function::scalar_function_repository& scalar_function_repository();

/**
 * @brief thread-safe accessor to the global configuration pool
 * @details the pool will be initialized on the first call and can be shared by multiple threads
 * @param arg updated configuration. Pass nullptr just to refer current value.
 * @return reference to the configuration
 */
takatori::util::maybe_shared_ptr<configuration> const& config_pool(takatori::util::maybe_shared_ptr<configuration> arg = nullptr);

/**
 * @brief thread-safe accessor to the global provider for scalar functions
 * @details the provider will be initialized on the first call and can be shared by multiple threads
 * @param arg updated provider. Pass nullptr just to refer current value.
 * @return reference to the function provider
 */
std::shared_ptr<yugawara::function::configurable_provider> const&
scalar_function_provider(std::shared_ptr<yugawara::function::configurable_provider> arg = nullptr);

/**
 * @brief thread-safe accessor to the kvs database
 * @details the container will be initialized on the first call and can be shared by multiple threads
 * @param arg new database. Pass nullptr just to refer current one.
 * @return reference to the kvs database
 */
std::shared_ptr<kvs::database> const& db(std::shared_ptr<kvs::database> arg = nullptr);

/**
 * @brief thread-safe accessor to the api::impl::database
 * @details the container will be initialized on the first call and can be shared by multiple threads
 * @param arg new database. Pass nullptr just to refer current one.
 * @return reference to the api::impl::database
 */
std::shared_ptr<api::impl::database> const& database_impl(std::shared_ptr<api::impl::database> arg = nullptr);

/**
 * @brief thread-safe accessor to the storage manager
 * @details the container will be initialized on the first call and can be shared by multiple threads
 * @param arg new storage manager. Pass nullptr just to refer current one.
 * @return reference to the storage manager
 */
std::shared_ptr<storage::storage_manager> const& storage_manager(std::shared_ptr<storage::storage_manager> arg = nullptr);

/**
 * @brief setter to the blob_relay_service
 * @details This set call is not thread-safe and must be called from a single thread.
 * @param arg new relay service instance.
 * @return reference to the blob_relay_service
 */
void relay_service(std::shared_ptr<data_relay_grpc::blob_relay::blob_relay_service> arg);

/**
 * @brief thread-safe getter to the blob_relay_service
 * @details once set by the setter above, it can be shared by multiple threads.
 * @return reference to the blob_relay_service
 */
std::shared_ptr<data_relay_grpc::blob_relay::blob_relay_service> const& relay_service();

}  // namespace jogasaki::global
