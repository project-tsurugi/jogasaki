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
#include <jogasaki/model/statement.h>
#include <jogasaki/configuration.h>

namespace jogasaki::scheduler {

/**
 * @brief Statement Scheduler
 */
class statement_scheduler {
public:
    /**
     * @brief creates a new instance.
     */
    statement_scheduler();

    /**
     * @brief destroys this object.
     */
    virtual ~statement_scheduler();

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    statement_scheduler(statement_scheduler const& other) = delete;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    statement_scheduler& operator=(statement_scheduler const& other) = delete;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    statement_scheduler(statement_scheduler&& other) noexcept = delete;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    statement_scheduler& operator=(statement_scheduler&& other) noexcept = delete;

    /**
     * @brief creates a new instance with given configuration
     */
    explicit statement_scheduler(std::shared_ptr<configuration> cfg);

    /**
     * @brief schedule the statement to run
     */
    void schedule(model::statement &s);

private:
    class impl;
    std::unique_ptr<impl> impl_;
};

} // namespace
