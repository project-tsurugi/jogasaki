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
#include <takatori/util/optional_ptr.h>

#include "step.h"

namespace jogasaki::model {

template <class T>
using optional_ptr = takatori::util::optional_ptr<T>;

/**
 * @brief step graph represents the model of the requested statement
 */
class graph {
public:
    /**
     * @return steps owned by this graph
     * Multiple calls return the steps in the same order, but the order is not meaningful (e.g. not ensured to be topologically sorted)
     */
    [[nodiscard]] virtual sequence_view<std::unique_ptr<step> const> steps() const = 0;

    /**
     * @brief find the step for the given identity in this graph
     * @return step if found
     */
    [[nodiscard]] virtual optional_ptr<step> find_step(step::identity_type id) = 0;

    /**
     * @brief creates a new instance.
     */
    graph() = default;

    /**
     * @brief destroys this object.
     */
    virtual ~graph() = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    graph(graph const& other) = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    graph& operator=(graph const& other) = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    graph(graph&& other) noexcept = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    graph& operator=(graph&& other) noexcept = default;
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(graph const& a, graph const& b) noexcept {
    return std::addressof(a) == std::addressof(b);
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(graph const& a, graph const& b) noexcept {
    return !(a == b);
}

}



