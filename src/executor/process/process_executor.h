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

#include <executor/process/step.h>
#include <executor/reader_container.h>
#include <executor/record_writer.h>
#include "processor_context.h"

namespace jogasaki::executor::process {

/**
 * @brief processor interface
 * @details this interface represents the sequence of procedures executed by the process
 */
class processor {
public:
    virtual ~processor() = 0;

    void initialize(processor_context& context);

    void run(){};
};

/**
 * @brief process executor
 * @details
 */
class process_executor {
public:
    /**
     * @brief construct new instance
     * @param partition index of the partition where the executor conduct
     * @param processor
     */
    process_executor(processor_context& context){
        (void)context;
    };

    void run() {
//        context_->initialize();
//        processor
//        context_->deinitialize();
    }
private:

};

}


