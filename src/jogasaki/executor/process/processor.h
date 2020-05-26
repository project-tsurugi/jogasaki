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

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>

namespace jogasaki::executor::process {

class processor_context;

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
 * @brief processor interface
 * @details the implementation represents the sequence of procedures executed by the process
 * The implementation is expected initialize itself with the supplied context and conduct the process
 * by retrieving the necessary I/O object and working area from context.
 */
class processor {
public:

    /**
     * @brief setter for the processor context
     * @param context the context for this processor
     */
    virtual void context(std::shared_ptr<processor_context> context) = 0;

    /**
     * @brief execute the processor body
     * @pre context has been provided by context() function
     * @return status code to notify caller of the execution status
     */
    virtual status run() = 0;

    /**
     * @brief destroy this object
     */
    virtual ~processor() = default;
};

}


