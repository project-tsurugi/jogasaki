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

namespace jogasaki::executor::process {

template <class T>
using sequence_view = takatori::util::sequence_view<T>;

/**
 * @brief processor context
 * @details this object is responsible to pass the IO objects to processor
 */
class processor_context {
public:
    using readers_list = sequence_view<reader_container>;
    using writers_list = sequence_view<record_writer>;
    /**
     * @brief initialize the context for the current environment(e.g. asssigned thread)
     * @details all other member functions are accessible only after initialized
     * @note context knows the partition that the associated processor belongs to
     */
    void initialize();

    /**
     * @brief accessor to main input readers
     * @return readers list lining up with the order of input ports
     */
    readers_list readers();

    /**
     * @brief accessor to sub-input readers
     * @return readers list lining up with the order of sub-input ports
     */
    readers_list subinput_readers();

    /**
     * @brief accessor to output writers
     * @return writers list lining up with the order of output ports
     */
    writers_list writers();

    //TODO provide output for emit/writer
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
    /**
     * @brief construct new instance
     * @param partition index of the partition where the executor conduct
     */
    process_executor();

    void run() {

    }
};

}


