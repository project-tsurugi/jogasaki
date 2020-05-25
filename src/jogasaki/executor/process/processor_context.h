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

template <class T>
using sequence_view = takatori::util::sequence_view<T>;

/**
 * @brief processor context interface
 * @details this object is responsible to pass the IO objects to processor
 */
class processor_context {
public:
    using reader_index = std::size_t;
    using writer_index = std::size_t;

    /**
     * @brief accessor to main/sub input readers
     * @return reader corresponding to the given index
     */
    virtual reader_container reader(reader_index idx) = 0;

    /**
     * @brief accessor to main output writers
     * @return writer corresponding to the given index
     */
    virtual record_writer* downstream_writer(writer_index idx) = 0;

    /**
     * @brief accessor to external writers (e.g. ones writing out record from Emit or Write)
     * @return external writer corresponding to the given index
     */
    virtual record_writer* external_writer(writer_index idx) = 0;

    /**
     * @brief destroy the object
     */
     virtual ~processor_context() = default;
};

}


