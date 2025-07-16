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

#include <cstddef>
#include <unordered_map>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/executor/function/incremental/aggregate_function_info.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::executor::function::incremental {

using takatori::util::maybe_shared_ptr;

/**
 * @brief aggregate functions repository
 * @details this is to hold ownership of pre-defined aggregate functions in one place
 */
class aggregate_function_repository {
public:
    using map_type = std::unordered_map<std::size_t, maybe_shared_ptr<aggregate_function_info>>;

    /**
     * @brief create new object
     */
    aggregate_function_repository() = default;

    /**
     * @brief register and store the aggregate function info
     * @param id the identifier used to distinguish the aggregate function
     * @param info the aggregate function info to store
     */
    void add(std::size_t id, maybe_shared_ptr<aggregate_function_info> info);

    /**
     * @brief find the repository with given id
     * @param id the identifier used to search
     * @return the aggregate function info if found.
     * @return nullptr if not found.
     */
    aggregate_function_info const* find(std::size_t id) const noexcept;

    /**
     * @brief clena up the repository
     */
    void clear() noexcept;

    /**
     * @brief return the number of function info registered
     */
    [[nodiscard]] std::size_t size() const noexcept;

private:
    map_type map_{};
};

}
