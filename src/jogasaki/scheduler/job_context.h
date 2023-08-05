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
#include <jogasaki/scheduler/request_detail.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>

namespace jogasaki {
class request_context;

namespace scheduler {

/**
 * @brief context object for the job
 * @details this class represents context information in the scope of the job scheduling
 */
class cache_align job_context {
public:
    /**
     * @brief job identifier
     */
    using job_id_type = std::size_t;

    /**
     * @brief constant to specify worker index is undefined
     */
    constexpr static std::size_t undefined_index = static_cast<std::size_t>(-1);

    /**
     * @brief constant to specify job id is undefined
     */
    constexpr static job_id_type undefined_id = static_cast<job_id_type>(-1);

    /**
     * @brief callback type at the end of job
     */
    using job_completion_callback = std::function<void(void)>;

    /**
     * @brief create default context object
     */
    job_context() = default;

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
     * @brief accessor for the preference worker of this job
     * @return the preferred worker id
     * @return `undefined_index` if it's not set
     */
    [[nodiscard]] std::atomic_size_t& preferred_worker_index() noexcept;

    /**
     * @brief accessor for the started flag used to mark whether any of the job tasks already ran
     * @return started flag
     */
    [[nodiscard]] std::atomic_bool& started() noexcept;

    /**
     * @brief reset the job context mutable variables to re-use
     */
    void reset() noexcept;

    /**
     * @brief setter for the callback
     */
    void callback(job_completion_callback callback) noexcept;

    /**
     * @brief accessor for completion callback
     * @return callback
     */
    [[nodiscard]] job_completion_callback& callback() noexcept;

    /**
     * @brief accessor for job context unique id
     * @return id value
     */
    [[nodiscard]] job_id_type id() const noexcept;

    /**
     * @brief setter for request detail
     */
    void request(std::shared_ptr<request_detail> arg) noexcept;

    /**
     * @brief getter for request detail
     */
    [[nodiscard]] std::shared_ptr<request_detail> const& request() const noexcept;

    /**
     * @brief accessor for hybrid execution mode
     * @details Hybrid scheduler uses this field to remember internal scheduler (serial or stealing) for the job.
     */
    [[nodiscard]] std::atomic<hybrid_execution_mode_kind>& hybrid_execution_mode() noexcept {
        return hybrid_execution_mode_;
    }
private:

    job_id_type id_{id_src_++};
    utils::latch completion_latch_{};
    cache_align std::atomic_bool completing_{false};
    cache_align std::atomic_size_t job_tasks_{};
    cache_align std::atomic_size_t preferred_worker_index_{undefined_index};
    cache_align std::atomic_bool started_{false};
    job_completion_callback callback_{};
    std::shared_ptr<request_detail> request_detail_{};
    cache_align std::atomic<hybrid_execution_mode_kind> hybrid_execution_mode_{hybrid_execution_mode_kind::undefined};

    static inline std::atomic_size_t id_src_{1UL << 32UL};  //NOLINT
};

}

}

