/*
 * Copyright 2018-2021 tsurugi project.
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

#include "context.h"
#include <tateyama/impl/core_affinity.h>

namespace tateyama {

/**
 * @brief task interface
 * @details this object is abstraction of task logic and state, and is used to submit task to scheduler.
 * Caller needs to inherit and implement the task content in the subclass.
 */
class cache_align task {
public:
    /**
     * @brief construct empty object
     */
    task() = default;

    /**
     * @brief copy construct
     */
    task(task const&) = default;

    /**
     * @brief move construct
     */
    task(task &&) = default;

    /**
     * @brief copy assign
     */
    task& operator=(task const&) = default;

    /**
     * @brief move assign
     */
    task& operator=(task &&) = default;

    /**
     * @brief destruct task
     */
    virtual ~task() = default;

    /**
     * @brief execute the task
     * @param ctx the context information on the worker that is running the task
     */
    virtual void operator()(context& ctx) = 0;
};

}

