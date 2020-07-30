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
#include <takatori/util/sequence_view.h>
#include "port.h"
#include "task.h"

namespace jogasaki::model {

class graph;

class step {
public:
    /**
     * @brief identity type whose value uniquely identifies step
     */
    using identity_type = std::size_t;

    /**
     * @brief index used to identify the port attached to this step
     * Each set of Main input ports, Sub input ports, and Output ports consists a category and indexes are 0-origin unique number within each category.
     */
    using port_index_type = std::size_t;

    /**
     * @brief creates a new instance.
     */
    step() = default;

    /**
     * @brief destroys this object.
     */
    virtual ~step() = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    step(step const& other) = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    step& operator=(step const& other) = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    step(step&& other) noexcept = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    step& operator=(step&& other) noexcept = default;

    /**
     * @return main input ports in the index order
     * The ports are owned by the step.
     */
    [[nodiscard]] virtual sequence_view<std::unique_ptr<port> const> input_ports() const = 0;

    /**
     * @return sub input ports in the index order
     * The ports are owned by the step.
     */
    [[nodiscard]] virtual sequence_view<std::unique_ptr<port> const> subinput_ports() const = 0;

    /**
     * @return output ports in the index order
     * The ports are owned by the step.
     */
    [[nodiscard]] virtual sequence_view<std::unique_ptr<port> const> output_ports() const = 0;

    /**
     * @brief request step to create main tasks required
     * @return list of 0 or more tasks that should be newly executed to process main input
     * The tasks are owned by the step.
     */
    [[nodiscard]] virtual sequence_view<std::shared_ptr<task>> create_tasks() = 0;

    /**
     * @brief request step to create prepare task to process input to the given sub-input port
     * @return list of 0 or a task that should be newly executed to process sub input
     * The tasks are owned by the step.
     */
    [[nodiscard]] virtual sequence_view<std::shared_ptr<task>> create_pretask(port_index_type subinput) = 0;

    /**
     * @return identity that uniquely identifies this step within the owner graph
     */
    [[nodiscard]] virtual identity_type id() const = 0;

    /**
     * @return graph that owns this step
     */
    [[nodiscard]] virtual graph* owner() const = 0;

    /**
     * @brief activate step context for data flow
     * @pre not activated yet, or already deactivated
     */
    virtual void activate() = 0;

    /**
     * @brief deactivate step context for data flow
     * @pre already activated and not yet deactivated
     */
    virtual void deactivate() = 0;

    /**
     * @brief notify the step of preparation completion
     * steps can use this notification to clean up resources for prepare tasks
     */
    virtual void notify_prepared() = 0;

    /**
     * @brief notify the step of main tasks completion
     * steps can use this notification to clean up resources for main tasks
     */
    virtual void notify_completed() = 0;

    /**
     * @return whether the step has sub input ports
     */
    [[nodiscard]] virtual bool has_subinput() = 0;

    /**
     * @brief dump step information
     */
    virtual std::ostream& write_to(std::ostream& out) const = 0;

};

/**
 * @brief dump step information to output stream
 */
inline std::ostream& operator<<(std::ostream& out, step const& value) {
    return value.write_to(out);
}

/**
 * @brief equality comparison operator
 */
inline bool operator==(step const& a, step const& b) noexcept {
    return std::addressof(a) == std::addressof(b);
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(step const& a, step const& b) noexcept {
    return !(a == b);
}

}


