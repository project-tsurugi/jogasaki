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

/**
 * @brief processor interface
 * @details the implementation represents the sequence of procedures executed by the process
 * The implementation is expected initialize itself with the passed context and to conduct the sequence
 * by retrieving the necessary I/O object from context.
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
     */
    virtual void run() = 0;

    /**
     * @brief destroy this object
     */
    virtual ~processor() = default;
};

}


