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

#include <atomic>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/utils/latch.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki {
class request_context;

namespace scheduler {

using takatori::util::maybe_shared_ptr;

/**
 * @brief context object for the job
 * @details this class represents context information in the scope of the job scheduling
 */
class cache_align job_context {
public:
    constexpr static std::size_t undefined_index = static_cast<std::size_t>(-1);

    using job_completion_callback = std::function<void(void)>;

    /**
     * @brief create default context object
     */
    job_context() = default;

    /**
     * @brief create default context object
     */
    job_context(
        std::size_t invoker_thread_cpu_id
    ) noexcept;

    ~job_context() = default;
    job_context(job_context const& other) = delete;
    job_context& operator=(job_context const& other) = delete;
    job_context(job_context&& other) noexcept = delete;
    job_context& operator=(job_context&& other) noexcept = delete;

    /**
     * @brief accessor for the completion latch used to notify client thread
     * @return latch to notify the job completion (released at the end of job)
     */
    [[nodiscard]] utils::latch& completion_latch() noexcept;

    /**
     * @brief accessor for the completion flag used to issue teardown task only once
     * @return completion flag
     */
    [[nodiscard]] std::atomic_bool& completing() noexcept;

    /**
     * @brief accessor for the atomic task counter used to check the number of remaining tasks
     * @return atomic task counter
     */
    [[nodiscard]] std::atomic_size_t& task_count() noexcept;

    /**
     * @brief accessor for the atomic task counter used to check the number of remaining tasks
     * @return atomic task counter
     */
    [[nodiscard]] std::atomic_size_t& index() noexcept;

    /**
     * @brief reset the job context mutable variables to re-use
     */
    void reset() noexcept;

    /**
     * @brief setter for the invoker thread cpu
     */
    void invoker_thread_cpu_id(std::size_t arg) noexcept;

    /**
     * @brief accessor for the invoker thread cpu
     * @return cpu id of the invoker thread
     */
    [[nodiscard]] std::size_t invoker_thread_cpu_id() const noexcept;

    /**
     * @brief setter for the callback
     */
    void callback(job_completion_callback callback) noexcept;

    /**
     * @brief accessor for completion callback
     * @return callback
     */
    [[nodiscard]] job_completion_callback& callback() noexcept;

private:

    utils::latch completion_latch_{};
    std::size_t invoker_thread_cpu_id_{};
    cache_align std::atomic_bool completing_{false};
    cache_align std::atomic_size_t job_tasks_{};
    cache_align std::atomic_size_t index_{undefined_index};
    job_completion_callback callback_{};
};

}

}

