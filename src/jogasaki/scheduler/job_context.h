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

namespace jogasaki {
class request_context;

namespace scheduler {

using takatori::util::maybe_shared_ptr;

class statement_scheduler;
class task_scheduler;

/**
 * @brief context object for the job
 * @details this class represents context information in the scope of the job scheduling
 */
class job_context {
public:
    /**
     * @brief create default context object
     */
    job_context() = default;

    /**
     * @brief create default context object
     */
    explicit job_context(
        maybe_shared_ptr<scheduler::statement_scheduler> statement
    ) noexcept :
        dag_scheduler_(std::move(statement))
    {}

    ~job_context() = default;
    job_context(job_context const& other) = delete;
    job_context& operator=(job_context const& other) = delete;
    job_context(job_context&& other) noexcept = delete;
    job_context& operator=(job_context&& other) noexcept = delete;

    /**
     * @brief setter for the dag scheduler
     */
    void dag_scheduler(maybe_shared_ptr<scheduler::statement_scheduler> arg) noexcept;

    /**
     * @brief accessor for the dag scheduler
     * @return dag scheduler shared within this request
     */
    [[nodiscard]] maybe_shared_ptr<scheduler::statement_scheduler> const& dag_scheduler() const noexcept;

    /**
     * @brief accessor for the completion latch used to notify client thread
     * @return latch to notify the job completion (released at the end of job)
     */
    [[nodiscard]] utils::latch& completion_latch() noexcept;

    /**
     * @brief accessor for the completion flag used to issue teardown task only once
     * @return completion flag
     */
    [[nodiscard]] std::atomic_bool& completing() noexcept {
        return completing_;
    }

    /**
     * @brief accessor for the atomic task counter used to check the number of remaining tasks
     * @return atomic task counter
     */
    [[nodiscard]] std::atomic_size_t& task_count() noexcept {
        return job_tasks_;
    }

    /**
     * @brief accessor for the atomic task counter used to check the number of remaining tasks
     * @return atomic task counter
     */
    [[nodiscard]] std::atomic_size_t& index() noexcept {
        return index_;
    }

    void reset() noexcept {
        completion_latch_.open();
        completing_.store(false);
        job_tasks_.store(0);
    }
private:
    maybe_shared_ptr<scheduler::statement_scheduler> dag_scheduler_{};
    utils::latch completion_latch_{};
    std::atomic_bool completing_{false};
    std::atomic_size_t job_tasks_{};
    std::atomic_size_t index_{};
};

}

}

