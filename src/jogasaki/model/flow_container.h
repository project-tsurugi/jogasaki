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

#include <vector>
#include <memory>

#include <takatori/util/downcast.h>

#include <jogasaki/model/flow.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::model {

using takatori::util::unsafe_downcast;

/**
 * @brief steps' flow context container
 * @details 0-origin index is assigned for each steap and it's used to identify the position
 * to store flow in this container.
 */
class cache_align flow_container {
public:
    using flows_type = std::vector<std::unique_ptr<flow>>;

    /**
     * @brief create empty object
     */
    flow_container() = default;

    /**
     * @brief create new object of given size
     * @param size the capacity of the container
     */
    explicit flow_container(
        std::size_t size
    );

    /**
     * @brief setter for the context at the given index
     * @param idx the index of the context
     * @param ctx the context to be stored
     * @return reference to the stored context
     */
    std::unique_ptr<flow>& set(std::size_t idx, std::unique_ptr<flow> ctx) noexcept;

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
    [[nodiscard]] flow* at(std::size_t idx) const noexcept;

private:
    flows_type flows_{};
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
[[nodiscard]] T* find_flow(std::size_t idx, flow_container& container) {
    return unsafe_downcast<T>(container.at(idx));
}

/**
 * @brief make operator context and store it in the context list held by task context (work context)
 * @tparam T the operator context type
 * @tparam Args arguments types to construct operator context
 * @param index the index to position the context in the context list
 * @param args arguments to construct operator context
 * @return pointer to the created/stored context
 */
template<class T, class ... Args>
[[nodiscard]] T* make_flow(flow_container& container, std::size_t index, Args&&...args) {
    auto& p = container.set(index, std::make_unique<T>(std::forward<Args>(args)...));
    return unsafe_downcast<T>(p.get());
}

}

