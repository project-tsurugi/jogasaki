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

#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include "work_context.h"
#include "scan_info.h"

namespace jogasaki::executor::process::abstract {

/**
 * @brief task context representing task assignment information and its running context
 * @details this object is responsible to provide scope of the work assigned to task, e.g. scan info,
 * input data reader, and transient work area
 *
 * Depending on whether the processor logic is driven by main/sub input or scan, readers() or
 * scan_info() functions are called to locate/retrieve the input data for the task.
 *
 * The knowledge about the number of I/O objects and its index (i.e. what port or exchange the i-th reader/writer
 * corresponds to) are shared with processor.
 *
 */
class task_context {
public:
    /// @brief index used to access readers
    using reader_index = std::size_t;

    /// @brief index used to access writers
    using writer_index = std::size_t;

    /**
     * @brief create new empty instance
     */
    task_context() = default;

    /**
     * @brief destroy the object
     */
    virtual ~task_context() = default;

    task_context(task_context const& other) = delete;
    task_context& operator=(task_context const& other) = delete;
    task_context(task_context&& other) noexcept = delete;
    task_context& operator=(task_context&& other) noexcept = delete;

    /**
     * @brief accessor to main/sub input readers
     * @details object in the "acquired" state will be borrowed corresponding to the given index.
     * The callers are responsible to call release() of the object when they finish using it.
     * The release() called second time are no-op, so they are safely called multiple times.
     * @param idx the requested reader's index
     * If this context is for the task processing main input(s), the index corresponds to the index of the main input.
     * If this context is for the task processing sub input, the parameter is ignored since
     * only one reader/input exists.
     * @return reader corresponding to the given index
     */
    [[nodiscard]] virtual reader_container reader(reader_index idx) = 0;

    /**
     * @brief accessor to main output writers
     * @details object in the "acquired" state will be borrowed corresponding to the given index.
     * The callers are responsible to call release() of the object when they finish using it.
     * The release() called second time are no-op, so they are safely called multiple times.
     * @param idx the requested writer's index, which corresponds to the index of output from the process step
     * @return writer corresponding to the given index
     * @attention for the task processing sub input, this
     */
    [[nodiscard]] virtual record_writer* downstream_writer(writer_index idx) = 0;

    /**
     * @brief accessor to external writers (e.g. ones writing out record from Emit or Write)
     * @details object in the "acquired" state will be borrowed corresponding to the given index.
     * The callers are responsible to call release() of the object when they finish using it.
     * The release() called second time are no-op, so they are safely called multiple times.
     * @param idx the requested external writer's index, which corresponds to the index of
     * external output from the process step
     * (the knowledge on the index of external output is shared with processor impl.)
     * @return external writer corresponding to the given index
     */
    [[nodiscard]] virtual record_writer* external_writer(writer_index idx) = 0;

    /**
     * @brief accessor to scan information that defines scan specification for the task
     * @details processor impl. knows the details scan_info and drives scan operation using it.
     * The details of scan_info is transparent to processor context.
     * @return scan info
     */
    [[nodiscard]] virtual class scan_info const* scan_info() = 0;

    /**
     * @brief setter of work context
     * @details processor impl. can store working data as work_context, which is transparent block to this object.
     */
    void work_context(std::unique_ptr<work_context> work_context);

    /**
     * @brief getter of work context
     * @details processor impl. can use this to access transient working area, which has been
     * passed by the setter above
     */
    [[nodiscard]] class work_context* work_context() const;

    /**
     * @brief detach and return the work_context held by this instance
     * @details callers can use this to finalize the work context when it finishes using the
     * context (i.e. the end of assigned work for the task)
     */
    [[nodiscard]] std::unique_ptr<class work_context> release_work();

private:
    std::unique_ptr<class work_context> work_context_{};
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(task_context const& a, task_context const& b) noexcept {
    return std::addressof(a) == std::addressof(b);
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(task_context const& a, task_context const& b) noexcept {
    return !(a == b);
}

}


