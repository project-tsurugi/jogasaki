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
#include <vector>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/process/impl/ops/context_base.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

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
     * @brief setter for the context at the given index
     * @param idx the index of the context
     * @param ctx the context to be stored
     * @return reference to the stored context
     */
    std::unique_ptr<context_base>& set(std::size_t idx, std::unique_ptr<context_base> ctx);

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

/**
 * @brief helper function to get the context of specified type `T`
 * @tparam T the type of the context
 * @param idx the index to find the context in the container
 * @param container the container to find the context
 * @return context object at the index of the container
 * @return nullptr if no context object is found
 */
template<class T>
[[nodiscard]] T* find_context(std::size_t idx, context_container& container) {
    return unsafe_downcast<T>(container.at(idx));
}

}

