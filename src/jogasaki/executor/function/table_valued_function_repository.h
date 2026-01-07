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

#include <cstddef>
#include <unordered_map>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/executor/function/table_valued_function_info.h>

namespace jogasaki::executor::function {

using takatori::util::maybe_shared_ptr;

/**
 * @brief repository for table-valued functions.
 * @details this class holds ownership of table-valued function definitions in one place.
 */
class table_valued_function_repository {
public:
    using map_type = std::unordered_map<std::size_t, maybe_shared_ptr<table_valued_function_info>>;

    /**
     * @brief constructs a new empty repository.
     */
    table_valued_function_repository() = default;

    /**
     * @brief registers and stores the table-valued function info.
     * @param id the identifier used to distinguish the function
     * @param info the function info to store
     */
    void add(std::size_t id, maybe_shared_ptr<table_valued_function_info> info);

    /**
     * @brief finds the function info with the given id.
     * @param id the identifier used to search
     * @return the function info if found
     * @return nullptr if not found
     */
    [[nodiscard]] table_valued_function_info const* find(std::size_t id) const noexcept;

    /**
     * @brief clears the repository.
     */
    void clear() noexcept;

    /**
     * @brief returns the number of function info registered.
     * @return the number of functions
     */
    [[nodiscard]] std::size_t size() const noexcept;

private:
    map_type map_{};
};

}  // namespace jogasaki::executor::function
