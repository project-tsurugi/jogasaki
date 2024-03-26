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

#include <cstddef>
#include <memory>
#include <vector>

#include <takatori/util/downcast.h>

#include <jogasaki/model/flow.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::model {

/**
 * @brief steps' flow context container
 * @details 0-origin index is assigned for each step and it's used to identify the position
 * to store flow in this container.
 */
class cache_align flow_repository {
public:
    using flows_type = std::vector<std::unique_ptr<flow>>;

    /**
     * @brief create empty object
     */
    flow_repository() = default;

    /**
     * @brief create new object of given size
     * @param size the capacity of the container
     */
    explicit flow_repository(
        std::size_t size
    );

    /**
     * @brief setter for the context at the given index
     * @param idx the index of the context
     * @param arg the context to be stored
     */
    void set(std::size_t idx, std::unique_ptr<flow> arg) noexcept;

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
     * @brief getter for the flow at the given index
     * @param idx the index to get the flow object
     * @return the flow object at the index
     * @return nullptr if no context object is stored
     */
    [[nodiscard]] flow* at(std::size_t idx) const noexcept;

private:
    flows_type flows_{};
};

}

