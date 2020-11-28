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

#include <memory>
#include <jogasaki/memory/page_pool.h>

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
[[nodiscard]] inline memory::page_pool& page_pool(pool_operation op = pool_operation::get) {
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

}

