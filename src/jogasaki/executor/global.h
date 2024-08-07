/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <takatori/util/maybe_shared_ptr.h>

// attention: making globals depend on lower domain slows down compile time
namespace jogasaki::executor::function::incremental {
class aggregate_function_repository;
}

namespace jogasaki::executor::function {
class aggregate_function_repository;
}

namespace jogasaki::executor::function {
class scalar_function_repository;
}

namespace jogasaki::memory {
class page_pool;
}

namespace jogasaki {
class configuration;
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

}

