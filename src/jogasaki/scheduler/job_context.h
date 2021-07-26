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
    job_context(
        maybe_shared_ptr<scheduler::statement_scheduler> statement,
        maybe_shared_ptr<task_scheduler> scheduler
    ) noexcept :
        dag_scheduler_(std::move(statement)),
        task_scheduler_(std::move(scheduler))
    {}


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
     * @brief setter for the dag scheduler
     */
    void request(maybe_shared_ptr<request_context> arg) noexcept {
        request_context_ = std::move(arg);
    }

    /**
     * @brief accessor for the dag scheduler
     * @return dag scheduler shared within this request
     */
    [[nodiscard]] maybe_shared_ptr<request_context> const& request() const noexcept {
        return request_context_;
    }

    /**
     * @brief setter for the dag scheduler
     */
    void scheduler(maybe_shared_ptr<task_scheduler> arg) noexcept {
        task_scheduler_ = arg;
    }

    /**
     * @brief accessor for the dag scheduler
     * @return dag scheduler shared within this request
     */
    [[nodiscard]] maybe_shared_ptr<task_scheduler> const& scheduler() const noexcept {
        return task_scheduler_;
    }
private:
    maybe_shared_ptr<scheduler::statement_scheduler> dag_scheduler_{};
    maybe_shared_ptr<request_context> request_context_{};
    maybe_shared_ptr<task_scheduler> task_scheduler_{};
    utils::latch completion_latch_{};
};

}

}

