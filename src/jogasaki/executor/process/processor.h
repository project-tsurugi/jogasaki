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

namespace jogasaki::executor::process {

class task_context;

/**
 * @brief processor return codes
 */
enum class status : std::size_t {

    /**
     * @brief processor completed with no errors
     */
    completed,

    /**
     * @brief processor completed with errors
     */
    completed_with_errors,

    /**
     * @brief processor suspended its task and is going to sleep
     * @attention not yet fully supported
     */
    to_sleep,

    /**
     * @brief processor suspended its task and is trying to yield to others
     * @attention not yet fully supported
     */
    to_yield,
};

/**
 * @brief processor logic interface
 * @details The implementation represents the data processing logic in the process step.
 *
 * A processor may represent a processing logic for data, that is sourced from either:
 * - main input(s)
 * - a sub input
 * - scan operation
 * The first/second cases are for the process step driven by take operator and the last one is by scan operator.
 * So a process step corresponds to the following processors:
 * - one processor for main inputs, or data from scan operation
 * - one processor per sub input
 *
 * The implementation is expected to conduct the process task, whose scope is determined by the I/O objects(readers/writers),
 * or scan_info retrieved from context passed with run().
 *
 * The processor must be re-entrant, i.e. required to allow calling run() from multiple threads for distinct tasks.
 * To save task specific working data across run() function call boundaries, processor can generate work_context
 * and keep it in processor_context.
 */
class processor {
public:

    /**
     * @brief execute the processor logic to conduct a task
     * @details execute the processor logic using the context, which gives information on assigned task
     * (e.g. the input data provided by reader, or scan details provided by scan_info)
     *
     * A task can be completed by one or more calls of run() with same context. Each call may be made from different thread.
     * But the calls for a task doesn't happen simultaneously, i.e. time interval of run() calls for a task don't over-wrap each other.
     *
     * @param context the context for the task conducted by this processor
     * @return status code to notify caller of the execution status
     */
    virtual status run(task_context* context) = 0;

    /**
     * @brief destroy this object
     */
    virtual ~processor() = default;
};

}


