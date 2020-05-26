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
#include "work_context.h"

namespace jogasaki::executor::process {

/**
 * @brief processor context abstract class
 * @details this object is responsible to provide the I/O objects and keep the working context for processor
 * The knowledge about the number of I/O objects and its index (i.e. what port or exchange the i-th reader/writer
 * corresponds to) are shared with processor.
 */
class processor_context {
public:
    /// @brief index used to access readers
    using reader_index = std::size_t;

    /// @brief index used to access writers
    using writer_index = std::size_t;

    /**
     * @brief create new empty instance
     */
    processor_context() = default;

    /**
     * @brief destroy the object
     */
    virtual ~processor_context() = default;

    processor_context(processor_context const& other) = delete;
    processor_context& operator=(processor_context const& other) = delete;
    processor_context(processor_context&& other) noexcept = delete;
    processor_context& operator=(processor_context&& other) noexcept = delete;

    /**
     * @brief accessor to main/sub input readers
     * @details internally stored reader or newly acquired reader will be returned, no need to release them one by one, but
     * use processor_context::release() function to do that at once for all resource
     * @return reader corresponding to the given index
     */
    virtual reader_container reader(reader_index idx) = 0;

    /**
     * @brief accessor to main output writers
     * @details internally stored reader or newly acquired reader will be returned, no need to release them one by one, but
     * use processor_context::release() function to do that at once for all resource
     * @return writer corresponding to the given index
     */
    virtual record_writer* downstream_writer(writer_index idx) = 0;

    /**
     * @brief accessor to external writers (e.g. ones writing out record from Emit or Write)
     * @details internally stored reader or newly acquired reader will be returned, no need to release them one by one, but
     * use processor_context::release() function to do that at once for all resource
     * @return external writer corresponding to the given index
     */
    virtual record_writer* external_writer(writer_index idx) = 0;

    /**
     * @brief setter of work context
     */
    void work_context(std::unique_ptr<work_context> work_context);

    /**
     * @brief getter of work context
     */
    [[nodiscard]] class work_context* work_context() const;

    /**
     * @brief release all resource (readers/writers and working context) attached to this instance
     * @details processor is required to call this when it ends using the context (i.e. the end of assigned work for the processor)
     */
    void release();

protected:
    /**
     * @brief request subclass to release all resource
     */
    virtual void do_release() = 0;

private:
    std::unique_ptr<class work_context> work_context_{};
};

}


