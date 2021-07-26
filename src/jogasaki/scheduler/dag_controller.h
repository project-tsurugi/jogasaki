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

#include <jogasaki/model/graph.h>
#include <jogasaki/configuration.h>
#include <jogasaki/scheduler/task_scheduler.h>

namespace jogasaki::scheduler {

/**
 * @brief Dependency Graph Scheduler
 */
class dag_controller {
public:
    /**
     * @brief creates a new instance.
     */
    dag_controller();

    /**
     * @brief destroys this object.
     */
    virtual ~dag_controller();

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    dag_controller(dag_controller const& other) = delete;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    dag_controller& operator=(dag_controller const& other) = delete;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    dag_controller(dag_controller&& other) noexcept = delete;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    dag_controller& operator=(dag_controller&& other) noexcept = delete;

    /**
     * @brief creates a new instance with given configuration
     */
    dag_controller(std::shared_ptr<configuration> cfg, task_scheduler& scheduler);

    /**
     * @brief creates a new instance with given configuration
     */
    explicit dag_controller(std::shared_ptr<configuration> cfg);

    /**
     * @brief schedule the graph to run
     */
    void schedule(model::graph &g);

    class impl;
    friend impl;
private:
    std::unique_ptr<impl> impl_;
};

} // namespace
