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
#include <memory>
#include <utility>
#include <vector>

#include <jogasaki/executor/process/impl/ops/context_base.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::process::impl::ops {

class context_base;

/**
 * @brief relational operator context container
 * @details 0-origin index is assigned for relational operator and it's used to identify the position
 * to store context in this container.
 */
class cache_align context_container {
public:
    using contexts_type = std::vector<std::unique_ptr<context_base>>;

    /**
     * @brief create empty object
     */
    context_container() = default;

    /**
     * @brief create new object of given size
     * @param size the capacity of the container
     */
    explicit context_container(
        std::size_t size
    );

    /**
     * @brief store the context at the given index if it is not assigned yet
     * @param idx the index of the context
     * @param ctx the context to be stored
     * @return the context stored at the index and whether the new context was stored
     * @throws std::logic_error if the index is out of range
     */
    [[nodiscard]] std::pair<context_base*, bool> try_emplace(
        std::size_t idx,
        std::unique_ptr<context_base>&& ctx);

    /**
     * @brief returns whether the context is stored at the given index
     * @param idx the index to check
     * @return true if context is already stored
     * @return false otherwise
     */
    [[nodiscard]] bool exists(std::size_t idx) const noexcept;

    /**
     * @brief return the capacity of the container
     */
    [[nodiscard]] std::size_t size() const noexcept;

    /**
     * @brief getter for the context at the given index
     * @param idx the index to get the context
     * @return the context object at the index
     * @return nullptr if no context object is stored
     */
    [[nodiscard]] context_base* at(std::size_t idx) const noexcept;

private:
    contexts_type contexts_{};
};

}
