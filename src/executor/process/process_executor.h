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

#include <executor/process/step.h>
#include <executor/reader_container.h>

namespace jogasaki::executor::process {

/**
 * @brief processor context
 * @details this object is responsible to pass the IO objects to processor
 */
class processor_context {
public:
    /**
     * @brief initialize the context for the current environment(e.g. asssigned thread)
     */
    void initialize();

    std::vector<reader_container> readers();


};

/**
 * @brief processor interface
 * @details this interface represents the sequence of procedures executed by the process
 */
class processor {
public:
    void initialize(processor_context& context);

};

/**
 * @brief process executor
 * @details
 */
class process_executor {
public:

};


}


