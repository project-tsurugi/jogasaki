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
#include "global.h"

#include <memory>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/executor/function/incremental/aggregate_function_repository.h>
#include <jogasaki/executor/function/aggregate_function_repository.h>

namespace jogasaki::global {

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

executor::function::incremental::aggregate_function_repository& incremental_aggregate_function_repository() {
    static executor::function::incremental::aggregate_function_repository repo{};
    return repo;
}

executor::function::aggregate_function_repository& aggregate_function_repository() {
    static executor::function::aggregate_function_repository repo{};
    return repo;
}

}

