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

#include <jogasaki/model/task.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/executor/common/write.h>
#include <tateyama/api/task_scheduler/context.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/common.h>
#include <jogasaki/scheduler/job_context.h>
#include "thread_params.h"

namespace jogasaki::scheduler {

/**
 * @brief task type kind
 */
enum class flat_task_kind : std::size_t {
    wrapped = 0,
    dag_events,
    bootstrap,
    teardown,
    write,
    bootstrap_resolving,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(flat_task_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = flat_task_kind;
    switch (value) {
        case kind::wrapped: return "wrapped"sv;
        case kind::dag_events: return "dag_events"sv;
        case kind::bootstrap: return "bootstrap"sv;
        case kind::teardown: return "teardown"sv;
        case kind::write: return "write"sv;
        case kind::bootstrap_resolving: return "bootstrap_resolving"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, flat_task_kind value) {
    return out << to_string_view(value);
}

template<auto Kind>
struct task_enum_tag_t {
    explicit task_enum_tag_t() = default;
};

template<auto Kind>
inline constexpr task_enum_tag_t<Kind> task_enum_tag {};

/**
 * @brief common task object
 * @details The task object used commonly for the jogasaki::scheduler::task_scheduler.
 * To support granule multi-threading, this object works some portiton of job scheduling, such as
 * bootstrapping the job, process dag scheduler internal events, and teardown the job.
 * The wrapped task supports wrapping jogasaki executors tasks (e.g. ones of process and exchange)
 */
class cache_align flat_task {
public:
    using identity_type = std::size_t;
    static constexpr identity_type undefined_id = static_cast<identity_type>(-1);

    /**
     * @brief create new object
     */
    flat_task() = default;

    ~flat_task() = default;
    flat_task(flat_task const& other) = default;
    flat_task& operator=(flat_task const& other) = default;
    flat_task(flat_task&& other) noexcept = default;
    flat_task& operator=(flat_task&& other) noexcept = default;

    /**
     * @brief construct new object wrapping jogasaki task
     * @param rctx the request context where the task belongs
     * @param origin the jogasaki executor task
     * @param sticky whether the task is sticky or not
     */
    flat_task(
        task_enum_tag_t<flat_task_kind::wrapped>,
        request_context* rctx,
        std::shared_ptr<model::task> origin
    ) noexcept;

    /**
     * @brief construct new object to run dag scheduler internal events
     * @param rctx the request context where the task belongs
     */
    flat_task(
        task_enum_tag_t<flat_task_kind::dag_events>,
        request_context* rctx
    ) noexcept;

    /**
     * @brief construct new object to bootstrap the job to run dag
     * @param rctx the request context where the task belongs
     * @param g the dag object to run as the job
     */
    flat_task(
        task_enum_tag_t<flat_task_kind::bootstrap>,
        request_context* rctx,
        model::graph& g
    ) noexcept;

    /**
     * @brief construct new object to teardown (finish processing) the job
     * @param rctx the request context where the task belongs
     */
    flat_task(
        task_enum_tag_t<flat_task_kind::teardown>,
        request_context* rctx
    ) noexcept;

    /**
     * @brief construct new object to run write
     * @param rctx the request context where the task belongs
     */
    flat_task(
        task_enum_tag_t<flat_task_kind::write>,
        request_context* rctx,
        executor::common::write* write
    ) noexcept;

    /**
     * @brief construct new object to resolve statement and bootstrap the job
     * @param jctx the job context where the task belongs
     * @param prepared the prepared statement used to resolve
     * @param parameters the parameters used to resolve
     * @param database the database used to resolve
     * @param tx the transaction to pass to the newly created request context
     * @param rctx [out] the shared ptr to keep the newly created object
     * @param result_store_container [out] the shared ptr to keep the newly created object
     * @param executable_statement_container [out] the shared ptr to keep the newly created object
     */
    flat_task(
        task_enum_tag_t<flat_task_kind::bootstrap_resolving>,
        job_context* jctx,
        api::prepared_statement const& prepared,
        api::parameter_set const& parameters,
        api::impl::database& database,
        std::shared_ptr<kvs::transaction> tx,
        std::shared_ptr<request_context>& rctx,
        std::unique_ptr<data::result_store>& result_store_container,
        std::unique_ptr<api::executable_statement>& executable_statement_container
    ) noexcept;

    /**
     * @brief getter for type kind
     */
    [[nodiscard]] constexpr flat_task_kind kind() const noexcept {
        return kind_;
    }

    /**
     * @brief getter for wrapped jogasaki executor task
     * @details this is only valid when the task kind is wrapped
     */
    [[nodiscard]] std::shared_ptr<model::task> const& origin() const noexcept;

    /**
     * @brief execute the task
     * @param ctx the tateyama task context, which provides info. about thread/worker running the task
     */
    void operator()(tateyama::api::task_scheduler::context& ctx);

    /**
     * @brief returns task id that uniquely identifies the task
     */
    [[nodiscard]] identity_type id() const;

    /**
     * @brief accessor to the job context that the task belongs to.
     */
    [[nodiscard]] job_context* job() const;

    /**
     * @brief dump the text representation of the value to output stream
     * @param out the target output stream
     * @param value the value to be output
     */
    friend std::ostream& operator<<(std::ostream& out, flat_task const& value) {
        return value.write_to(out);
    }

    /**
     * @brief returns whether the task is sticky
     */
    [[nodiscard]] bool sticky() const noexcept;

    /**
     * @brief accessor to the job context that the task belongs to.
     */
    [[nodiscard]] request_context* req_context() const noexcept;

private:
    flat_task_kind kind_{};
    request_context* req_context_{};
    std::shared_ptr<model::task> origin_{};
    model::graph* graph_{};
    executor::common::write* write_{};
    bool sticky_{};

    // members for resolve
    api::prepared_statement const* prepared_{};
    api::parameter_set const* parameters_{};
    api::impl::database* database_{};
    std::shared_ptr<kvs::transaction> tx_{};
    std::shared_ptr<request_context>* request_context_container_{};
    std::unique_ptr<data::result_store>* result_store_container_{};
    std::unique_ptr<api::executable_statement>* executable_statement_container_{};

    /**
     * @return true if job completes together with the task
     * @return false if only task completes
     */
    bool execute(tateyama::api::task_scheduler::context& ctx);

    void bootstrap(tateyama::api::task_scheduler::context& ctx);
    void dag_schedule();

    /**
     * @return true if the teardown task completes
     * @return false if the teardown task is rescheduled
     */
    bool teardown();
    void bootstrap_resolving(tateyama::api::task_scheduler::context& ctx);
    void resolve(tateyama::api::task_scheduler::context& ctx);

    void write();
    void finish_job();

    std::ostream& write_to(std::ostream& out) const {
        using namespace std::string_view_literals;
        return out << "task[id="sv << std::to_string(static_cast<identity_type>(id())) << "]"sv;
    }

};

}
