/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <ostream>
#include <string_view>
#include <utility>

#include <takatori/util/maybe_shared_ptr.h>
#include <tateyama/task_scheduler/context.h>

#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/common.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/common/write_statement.h>
#include <jogasaki/executor/file/loader.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/hex.h>
#include <jogasaki/utils/interference_size.h>

#include "thread_params.h"

namespace jogasaki::api::impl {
class database;
}

namespace jogasaki::scheduler {

/**
 * @brief task type kind
 */
enum class flat_task_kind : std::size_t {
    wrapped = 0,
    dag_events,
    bootstrap,
    teardown,
    load,
    write,
    resolve,
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
        case kind::load: return "load"sv;
        case kind::write: return "write"sv;
        case kind::resolve: return "resolve"sv;
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

using callback = api::transaction_handle::callback;

// original is in executor.h, but defining here in order to avoid cycle in headers inclusion
using error_info_stats_callback = std::function<
    void(status, std::shared_ptr<error::error_info>, std::shared_ptr<request_statistics>)
>;

struct statement_context {
    statement_context(
        api::statement_handle prepared,
        std::shared_ptr<api::parameter_set const> parameters,
        api::impl::database* database,
        std::shared_ptr<transaction_context> tx,
        error_info_stats_callback cb
    ) noexcept :
        prepared_(prepared),
        parameters_(std::move(parameters)),
        database_(database),
        tx_(std::move(tx)),
        callback_(std::move(cb))
    {}

    api::statement_handle prepared_{};  //NOLINT
    std::shared_ptr<api::parameter_set const> parameters_{};  //NOLINT
    api::impl::database* database_{};  //NOLINT
    std::shared_ptr<transaction_context> tx_{};  //NOLINT
    std::unique_ptr<api::executable_statement> executable_statement_{};  //NOLINT
    error_info_stats_callback callback_{};  //NOLINT
};

void submit_teardown(request_context& req_context, bool force = false, bool try_on_suspended_worker = false);

bool check_or_submit_teardown(
    request_context& req_context,
    bool calling_from_task = false,
    bool force = false,
    bool try_on_suspended_worker = false
);

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

    using clock = std::chrono::high_resolution_clock;

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
     * @details job context has the counter for the non-teardown tasks and teardown waits for them to finish by
     * duplicating teardown task and re-scheduling it. Finally the teardown task makes job completion callback and
     * erases the globally stored job context.
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
        executor::common::write_statement* write
    ) noexcept;

    /**
     * @brief construct new object to resolve statement and bootstrap the job
     * @param rctx the request context where the task belongs
     * @param sctx the statement context
     */
    flat_task(
        task_enum_tag_t<flat_task_kind::resolve>,
        std::shared_ptr<request_context> rctx,
        std::shared_ptr<statement_context> sctx
    ) noexcept;

    /**
     * @brief construct new object to load
     * @param rctx the request context where the task belongs
     * @param ldr loader to conduct the main logic of load
     */
    flat_task(
        task_enum_tag_t<flat_task_kind::load>,
        request_context* rctx,
        std::shared_ptr<executor::file::loader> ldr
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
    void operator()(tateyama::task_scheduler::context& ctx);

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
    std::size_t id_{undefined_id};
    flat_task_kind kind_{};
    maybe_shared_ptr<request_context> req_context_{};
    std::shared_ptr<model::task> origin_{};
    model::graph* graph_{};
    executor::common::write_statement* write_{};
    bool sticky_{};
    std::shared_ptr<statement_context> sctx_{};
    std::shared_ptr<executor::file::loader> loader_{};

    static inline std::atomic_size_t id_src_{};  //NOLINT

    /**
     * @return true if job completes together with the task
     * @return false if only task completes
     */
    bool execute(tateyama::task_scheduler::context& ctx);

    void bootstrap(tateyama::task_scheduler::context& ctx);
    void dag_schedule();

    void resolve(tateyama::task_scheduler::context& ctx);

    bool write();
    bool load();
    bool execute_wrapped();
    void resubmit(request_context& req_context);

    std::ostream& write_to(std::ostream& out) const {
        using namespace std::string_view_literals;
        return out << "task[id="sv << utils::hex(id()) << "]"sv;
    }

};

/**
 * @brief function to check job is ready to finish
 * @return true if there is no other tasks for the job and completion is ready
 * @return false otherwise
 */
bool ready_to_finish(job_context& job, bool calling_from_task);

/**
 * @brief finish the job
 * @details this function doesn't check any condition for teardown, so use only when you are sure the job is ready to finish
 */
void finish_job(request_context& req_context);

/**
 * @brief process dag scheduler internal events to proceed dag state
 */
void dag_schedule(request_context& req_context);

void print_task_diagnostic(flat_task const& t, std::ostream& os);

}
