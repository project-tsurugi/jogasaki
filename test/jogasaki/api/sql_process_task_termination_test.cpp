/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/executable_statement.h>
#include <jogasaki/api/impl/request_context_factory.h>
#include <jogasaki/api/impl/result_set.h>
#include <jogasaki/api/impl/result_store_channel.h>
#include <jogasaki/api/result_set_iterator.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/configuration.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/abstract/process_executor.h>
#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/process_executor.h>
#include <jogasaki/executor/process/impl/task_context.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/plan/executable_statement.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_info.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/status.h>
#include <jogasaki/termination_state.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/transaction_state_kind.h>
#include <jogasaki/utils/cancel_request.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;

using takatori::util::maybe_shared_ptr;
using api::impl::get_impl;

namespace abstract = executor::process::abstract;

/**
 * @brief the fault injected into the process executor of a process step
 */
enum class fault_kind {
    none,
    // report an execution error when each process task finishes its work
    error_on_completion,
    // cancel the request when the first process task finishes its work
    cancel_on_completion,
    // yield task_count times before running work (a shared injection budget)
    yield_before_run,
    error_after_partial_output,
};

/**
 * @brief counters observed on the process executor of a process step
 */
struct process_probe {
    std::atomic_size_t contexts{};
    std::atomic_size_t runs{};
    std::atomic_size_t terminal_runs{};
    std::atomic_size_t injected{};
    std::atomic_size_t real_yields{};
};

/**
 * @brief process executor wrapping the default one to inject faults at the process task boundary
 * @details the default executor keeps managing task contexts and writer seats, so the injected
 * behavior is limited to the status reported to executor::process::task.
 */
class fault_injecting_executor : public abstract::process_executor {
public:
    fault_injecting_executor(
        std::shared_ptr<abstract::process_executor> inner,
        request_context& rctx,
        fault_kind kind,
        std::size_t task_count,
        std::shared_ptr<process_probe> probe
    ) :
        inner_(std::move(inner)),
        rctx_(std::addressof(rctx)),
        kind_(kind),
        remaining_yields_(kind == fault_kind::yield_before_run ? task_count : 0),
        probe_(std::move(probe))
    {}

    status run() override {
        ++probe_->runs;
        if (consume_yield()) {
            ++probe_->injected;
            return status::to_yield;
        }
        auto rc = inner_->run();
        if (rc == status::to_yield) ++probe_->real_yields;
        if (rc != status::completed && rc != status::completed_with_errors) {
            return rc;
        }
        ++probe_->terminal_runs;
        switch (kind_) {
            case fault_kind::error_on_completion:
                ++probe_->injected;
                set_error_context(
                    *rctx_,
                    error_code::sql_execution_exception,
                    "injected process task failure",
                    jogasaki::status::err_unknown
                );
                return status::completed_with_errors;
            case fault_kind::cancel_on_completion:
                if (! cancelled_.exchange(true)) {
                    ++probe_->injected;
                    cancel_request(*rctx_);
                }
                return rc;
            default:
                return rc;
        }
    }

private:
    std::shared_ptr<abstract::process_executor> inner_{};
    request_context* rctx_{};
    fault_kind kind_{};
    std::atomic_size_t remaining_yields_{};
    std::atomic_bool cancelled_{};
    std::shared_ptr<process_probe> probe_{};

    bool consume_yield() {
        auto remaining = remaining_yields_.load();
        while (remaining > 0) {
            if (remaining_yields_.compare_exchange_weak(remaining, remaining - 1)) {
                return true;
            }
        }
        return false;
    }
};

/** @brief Fail after the real scan has written rows and yielded, before task completion. */
class partial_output_fault_processor final : public abstract::processor {
public:
    partial_output_fault_processor(std::shared_ptr<abstract::processor> inner,
        std::shared_ptr<process_probe> probe) : inner_(std::move(inner)), probe_(std::move(probe)) {}
    abstract::status run(abstract::task_context* context) override {
        auto result = inner_->run(context);
        if (result != abstract::status::to_yield) return result;
        auto* actual = dynamic_cast<executor::process::impl::task_context*>(context);
        ++probe_->injected;
        set_error_context(*actual->req_context(), error_code::sql_execution_exception,
            "injected failure after partial scan output", jogasaki::status::err_unknown);
        return abstract::status::completed_with_errors;
    }
private:
    std::shared_ptr<abstract::processor> inner_{};
    std::shared_ptr<process_probe> probe_{};
};

/**
 * @brief result of a query execution observed from the job completion callback
 */
struct query_outcome {
    status status_{};
    std::shared_ptr<error::error_info> error_{};
    std::size_t completions_{};
    std::size_t task_count_on_completion_{};
    std::size_t rows_{};
    std::vector<std::pair<std::int32_t, std::int64_t>> values_{};
    bool flows_deactivated_{true};
};

class ApiTestContext final : public api_test_base {
public:
    bool to_explain() override { return false; }
};

/**
 * @brief tests the termination paths of process tasks (normal, execution error, cancel, yield)
 * @details faults are injected through process::step::executor_factory() into a two-process query
 * (scan -> aggregate exchange -> aggregate/emit) and the query is executed on 4 workers.
 * The parameter switches inplace DAG scheduling.
 */
class ProcessTaskTerminationTest : public ::testing::TestWithParam<bool> {
public:

    api::impl::database& database() { return *db_context_.db_impl(); }

    virtual bool exercise_partial_output() const noexcept { return false; }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->thread_pool_size(4);
        cfg->default_partitions(4);
        cfg->inplace_dag_schedule(GetParam());
        if (exercise_partial_output()) {
            cfg->scan_block_size(1);
            cfg->scan_yield_interval(0);
        }
        db_context_.db_setup(cfg);
        db_context_.execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    }

    void TearDown() override {
        db_context_.db_teardown();
    }

    void insert_rows(std::size_t count) {
        for (std::size_t i = 0; i < count; ++i) {
            db_context_.execute_statement("INSERT INTO t VALUES (" + std::to_string(i) + ", " + std::to_string(i) + ")");
        }
    }

    void prepare(
        std::unique_ptr<api::executable_statement>& stmt,
        executor::process::step*& upstream,
        executor::process::step*& downstream
    ) {
        std::shared_ptr<error::error_info> info{};
        ASSERT_EQ(status::ok, get_impl(database()).create_executable("SELECT c1, COUNT(*) FROM t GROUP BY c1", stmt, info));
        auto& body = dynamic_cast<api::impl::executable_statement&>(*stmt).body();
        ASSERT_TRUE(body->is_execute());
        auto& g = dynamic_cast<executor::common::execute&>(*body->operators()).operators();
        upstream = nullptr;
        downstream = nullptr;
        std::size_t process_steps = 0;
        for (auto&& s : g.steps()) {
            auto* p = dynamic_cast<executor::process::step*>(s.get());
            if (p == nullptr) {
                continue;
            }
            ++process_steps;
            if (p->input_ports().empty()) {
                upstream = p;
            } else {
                downstream = p;
            }
        }
        // expecting scan process -> aggregate exchange -> aggregate/emit process
        ASSERT_EQ(2, process_steps);
        ASSERT_TRUE(upstream);
        ASSERT_TRUE(downstream);
    }

    static void inject(executor::process::step& step, fault_kind kind, std::shared_ptr<process_probe> const& probe) {
        step.executor_factory(std::make_shared<abstract::process_executor_factory>(
            [kind, probe](
                std::shared_ptr<abstract::processor> processor,
                std::vector<std::shared_ptr<abstract::task_context>> contexts
            ) -> std::shared_ptr<abstract::process_executor> {
                auto task_count = contexts.size();
                auto* ctx = dynamic_cast<executor::process::impl::task_context*>(contexts.front().get());
                probe->contexts += task_count;
                if (kind == fault_kind::error_after_partial_output) {
                    processor = std::make_shared<partial_output_fault_processor>(std::move(processor), probe);
                }
                auto inner = executor::process::impl::default_process_executor_factory()(
                    std::move(processor),
                    std::move(contexts)
                );
                return std::make_shared<fault_injecting_executor>(
                    std::move(inner),
                    *ctx->req_context(),
                    kind,
                    task_count,
                    probe
                );
            }
        ));
    }

    void run(api::executable_statement& stmt, api::transaction_handle tx, query_outcome& out) {
        auto store = std::make_unique<data::result_store>();
        auto ch = std::make_shared<api::impl::result_store_channel>(maybe_shared_ptr{store.get()});
        auto rctx = api::impl::create_request_context(
            get_impl(database()),
            api::get_transaction_context(tx),
            ch,
            std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
            request_info{},
            true,
            nullptr
        );
        auto* raw = rctx.get();
        ASSERT_TRUE(executor::execute_async_on_context(
            get_impl(database()),
            rctx,
            maybe_shared_ptr{std::addressof(stmt)},
            [&out, raw](
                status st,
                std::shared_ptr<error::error_info> info,  //NOLINT(performance-unnecessary-value-param)
                std::shared_ptr<request_statistics> const& /* stats */
            ) {
                out.status_ = st;
                out.error_ = std::move(info);
                ++out.completions_;
                // job context is still alive while the job callback is running
                out.task_count_on_completion_ = raw->job()->task_count().load();
                if (auto flows = raw->flows()) {
                    for (std::size_t i = 0; i < flows->size(); ++i) {
                        if (flows->exists(i)) out.flows_deactivated_ = false;
                    }
                }
            },
            true
        ));
        api::impl::result_set rs{std::move(store)};
        auto it = rs.iterator();
        while (it->has_next()) {
            auto* record = it->next();
            out.values_.emplace_back(record->get_int4(0), record->get_int8(1));
            ++out.rows_;
        }
    }

    /**
     * @brief verify the job terminates once without leaving tasks or transaction users behind
     */
    static void expect_terminated(query_outcome const& out, transaction_context& tctx) {
        EXPECT_EQ(1, out.completions_);
        EXPECT_EQ(0, out.task_count_on_completion_);
        EXPECT_TRUE(out.flows_deactivated_);
        EXPECT_EQ(0, tctx.termination_mgr().state().task_use_count());
    }

    /**
     * @brief verify the terminal run count matches the context count
     */
    static void expect_terminal_count_matches_contexts(process_probe const& probe) {
        EXPECT_LT(0, probe.contexts.load());
        EXPECT_EQ(probe.contexts.load(), probe.terminal_runs.load());
    }

private:
    ApiTestContext db_context_{};
};

class ProcessPartialOutputTerminationTest : public ProcessTaskTerminationTest {
public:
    bool exercise_partial_output() const noexcept override { return true; }
};

TEST_P(ProcessTaskTerminationTest, NormalZeroRows) {
    std::unique_ptr<api::executable_statement> stmt{};
    executor::process::step* upstream{};
    executor::process::step* downstream{};
    ASSERT_NO_FATAL_FAILURE(prepare(stmt, upstream, downstream));
    auto up = std::make_shared<process_probe>();
    auto down = std::make_shared<process_probe>();
    inject(*upstream, fault_kind::none, up);
    inject(*downstream, fault_kind::none, down);

    auto tx = utils::create_transaction(database());
    query_outcome out{};
    run(*stmt, *tx, out);
    EXPECT_EQ(status::ok, out.status_);
    EXPECT_FALSE(out.error_);
    EXPECT_EQ(0, out.rows_);
    expect_terminated(out, *api::get_transaction_context(*tx));
    expect_terminal_count_matches_contexts(*up);
    expect_terminal_count_matches_contexts(*down);
    EXPECT_EQ(up->terminal_runs.load(), up->runs.load());
    EXPECT_EQ(down->terminal_runs.load(), down->runs.load());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_P(ProcessTaskTerminationTest, NormalMultipleRows) {
    insert_rows(10);
    std::unique_ptr<api::executable_statement> stmt{};
    executor::process::step* upstream{};
    executor::process::step* downstream{};
    ASSERT_NO_FATAL_FAILURE(prepare(stmt, upstream, downstream));
    auto up = std::make_shared<process_probe>();
    auto down = std::make_shared<process_probe>();
    inject(*upstream, fault_kind::none, up);
    inject(*downstream, fault_kind::none, down);

    auto tx = utils::create_transaction(database());
    query_outcome out{};
    run(*stmt, *tx, out);
    EXPECT_EQ(status::ok, out.status_);
    EXPECT_FALSE(out.error_);
    EXPECT_EQ(10, out.rows_);
    expect_terminated(out, *api::get_transaction_context(*tx));
    expect_terminal_count_matches_contexts(*up);
    expect_terminal_count_matches_contexts(*down);
    EXPECT_EQ(4, down->contexts.load());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_P(ProcessTaskTerminationTest, ErrorInUpstreamTaskSkipsDownstream) {
    // an execution error completes the DAG task, and the downstream process step creates tasks but never runs them
    insert_rows(10);
    std::unique_ptr<api::executable_statement> stmt{};
    executor::process::step* upstream{};
    executor::process::step* downstream{};
    ASSERT_NO_FATAL_FAILURE(prepare(stmt, upstream, downstream));
    auto up = std::make_shared<process_probe>();
    auto down = std::make_shared<process_probe>();
    inject(*upstream, fault_kind::error_on_completion, up);
    inject(*downstream, fault_kind::none, down);

    auto tx = utils::create_transaction(database());
    query_outcome out{};
    run(*stmt, *tx, out);
    EXPECT_EQ(status::err_unknown, out.status_);
    ASSERT_TRUE(out.error_);
    EXPECT_EQ(error_code::sql_execution_exception, out.error_->code());
    EXPECT_EQ(0, out.rows_);
    expect_terminated(out, *api::get_transaction_context(*tx));
    expect_terminal_count_matches_contexts(*up);
    EXPECT_EQ(up->contexts.load(), up->injected.load());
    EXPECT_GT(down->contexts.load(), 0);
    EXPECT_EQ(0, down->runs.load());
    EXPECT_EQ(status::ok, tx->abort());
}

TEST_P(ProcessTaskTerminationTest, ErrorInParallelTasks) {
    // every partition of the downstream step reports an error; each DAG task still completes exactly once
    insert_rows(10);
    std::unique_ptr<api::executable_statement> stmt{};
    executor::process::step* upstream{};
    executor::process::step* downstream{};
    ASSERT_NO_FATAL_FAILURE(prepare(stmt, upstream, downstream));
    auto up = std::make_shared<process_probe>();
    auto down = std::make_shared<process_probe>();
    inject(*upstream, fault_kind::none, up);
    inject(*downstream, fault_kind::error_on_completion, down);

    auto tx = utils::create_transaction(database());
    query_outcome out{};
    run(*stmt, *tx, out);
    EXPECT_EQ(status::err_unknown, out.status_);
    ASSERT_TRUE(out.error_);
    EXPECT_EQ(error_code::sql_execution_exception, out.error_->code());
    expect_terminated(out, *api::get_transaction_context(*tx));
    expect_terminal_count_matches_contexts(*up);
    expect_terminal_count_matches_contexts(*down);
    EXPECT_EQ(4, down->contexts.load());
    EXPECT_EQ(4, down->injected.load());
    EXPECT_EQ(status::ok, tx->abort());
}

TEST_P(ProcessTaskTerminationTest, CancelInUpstreamTask) {
    // cancel aborts the transaction after the last in-transaction task leaves, and the downstream step does not run
    insert_rows(10);
    std::unique_ptr<api::executable_statement> stmt{};
    executor::process::step* upstream{};
    executor::process::step* downstream{};
    ASSERT_NO_FATAL_FAILURE(prepare(stmt, upstream, downstream));
    auto up = std::make_shared<process_probe>();
    auto down = std::make_shared<process_probe>();
    inject(*upstream, fault_kind::cancel_on_completion, up);
    inject(*downstream, fault_kind::none, down);

    auto tx = utils::create_transaction(database());
    auto tctx = api::get_transaction_context(*tx);
    query_outcome out{};
    run(*stmt, *tx, out);
    EXPECT_EQ(status::request_canceled, out.status_);
    ASSERT_TRUE(out.error_);
    EXPECT_EQ(error_code::request_canceled, out.error_->code());
    EXPECT_EQ(0, out.rows_);
    expect_terminated(out, *tctx);
    expect_terminal_count_matches_contexts(*up);
    EXPECT_EQ(1, up->injected.load());
    EXPECT_GT(down->contexts.load(), 0);
    EXPECT_EQ(0, down->runs.load());
    EXPECT_TRUE(tctx->termination_mgr().state().going_to_abort());
    EXPECT_EQ(transaction_state_kind::aborted, tctx->state());
    EXPECT_NE(status::ok, tx->commit());
}

TEST_P(ProcessTaskTerminationTest, YieldBeforeRun) {
    // Tasks yield task_count times in total; yielding must not complete the DAG task or job.
    insert_rows(10);
    std::unique_ptr<api::executable_statement> stmt{};
    executor::process::step* upstream{};
    executor::process::step* downstream{};
    ASSERT_NO_FATAL_FAILURE(prepare(stmt, upstream, downstream));
    auto up = std::make_shared<process_probe>();
    auto down = std::make_shared<process_probe>();
    inject(*upstream, fault_kind::yield_before_run, up);
    inject(*downstream, fault_kind::yield_before_run, down);

    auto tx = utils::create_transaction(database());
    query_outcome out{};
    run(*stmt, *tx, out);
    EXPECT_EQ(status::ok, out.status_);
    EXPECT_FALSE(out.error_);
    EXPECT_EQ(10, out.rows_);
    expect_terminated(out, *api::get_transaction_context(*tx));
    expect_terminal_count_matches_contexts(*up);
    expect_terminal_count_matches_contexts(*down);
    EXPECT_EQ(up->contexts.load(), up->injected.load());
    EXPECT_EQ(down->contexts.load(), down->injected.load());
    EXPECT_EQ(up->terminal_runs.load() + up->injected.load(), up->runs.load());
    EXPECT_EQ(down->terminal_runs.load() + down->injected.load(), down->runs.load());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_P(ProcessTaskTerminationTest, RepeatedTerminations) {
    // run the termination paths back to back on the same workers to detect leftovers from preceding jobs
    insert_rows(10);
    struct scenario {
        fault_kind upstream_;
        fault_kind downstream_;
        status expected_;
    };
    std::vector<scenario> scenarios{
        {fault_kind::none, fault_kind::none, status::ok},
        {fault_kind::error_on_completion, fault_kind::none, status::err_unknown},
        {fault_kind::yield_before_run, fault_kind::yield_before_run, status::ok},
        {fault_kind::none, fault_kind::error_on_completion, status::err_unknown},
        {fault_kind::cancel_on_completion, fault_kind::none, status::request_canceled},
    };
    for (std::size_t i = 0; i < 20; ++i) {
        auto& sc = scenarios[i % scenarios.size()];
        SCOPED_TRACE("iteration " + std::to_string(i));
        std::unique_ptr<api::executable_statement> stmt{};
        executor::process::step* upstream{};
        executor::process::step* downstream{};
        ASSERT_NO_FATAL_FAILURE(prepare(stmt, upstream, downstream));
        auto up = std::make_shared<process_probe>();
        auto down = std::make_shared<process_probe>();
        inject(*upstream, sc.upstream_, up);
        inject(*downstream, sc.downstream_, down);

        auto tx = utils::create_transaction(database());
        query_outcome out{};
        run(*stmt, *tx, out);
        EXPECT_EQ(sc.expected_, out.status_);
        expect_terminated(out, *api::get_transaction_context(*tx));
        expect_terminal_count_matches_contexts(*up);
        if (sc.expected_ == status::ok) {
            EXPECT_EQ(10, out.rows_);
            ASSERT_EQ(status::ok, tx->commit());
        } else {
            (void) tx->abort();
        }
    }
}


TEST_P(ProcessPartialOutputTerminationTest, RealYieldAfterPartialOutputResumesWithoutDuplicateRows) {
    insert_rows(10);
    std::unique_ptr<api::executable_statement> stmt{};
    executor::process::step* upstream{};
    executor::process::step* downstream{};
    ASSERT_NO_FATAL_FAILURE(prepare(stmt, upstream, downstream));
    auto up = std::make_shared<process_probe>();
    auto down = std::make_shared<process_probe>();
    inject(*upstream, fault_kind::none, up);
    inject(*downstream, fault_kind::none, down);
    auto tx = utils::create_transaction(database());
    query_outcome out{};
    run(*stmt, *tx, out);
    EXPECT_EQ(status::ok, out.status_);
    EXPECT_EQ(10, out.rows_);
    EXPECT_GT(up->real_yields.load(), 0);
    // COUNT(*) also detects replayed input that a row-count-only assertion would miss.
    std::sort(out.values_.begin(), out.values_.end());
    std::vector<std::pair<std::int32_t, std::int64_t>> expected{};
    expected.reserve(10);
    for (std::int32_t i = 0; i < 10; ++i) {
        expected.emplace_back(i, 1);
    }
    EXPECT_EQ(expected, out.values_);
    expect_terminated(out, *api::get_transaction_context(*tx));
    expect_terminal_count_matches_contexts(*up);
    expect_terminal_count_matches_contexts(*down);
    EXPECT_EQ(status::ok, tx->commit());
}
TEST_P(ProcessPartialOutputTerminationTest, ErrorAfterPartialOutputTerminatesAndReleasesFlows) {
    insert_rows(10);
    std::unique_ptr<api::executable_statement> stmt{};
    executor::process::step* upstream{};
    executor::process::step* downstream{};
    ASSERT_NO_FATAL_FAILURE(prepare(stmt, upstream, downstream));
    auto up = std::make_shared<process_probe>();
    auto down = std::make_shared<process_probe>();
    inject(*upstream, fault_kind::error_after_partial_output, up);
    inject(*downstream, fault_kind::none, down);
    auto tx = utils::create_transaction(database());
    query_outcome out{};
    run(*stmt, *tx, out);
    EXPECT_EQ(status::err_unknown, out.status_);
    EXPECT_GT(up->injected.load(), 0);
    EXPECT_EQ(0, out.rows_);
    EXPECT_EQ(0, down->runs.load());
    expect_terminated(out, *api::get_transaction_context(*tx));
    expect_terminal_count_matches_contexts(*up);
    EXPECT_EQ(status::ok, tx->abort());
}

INSTANTIATE_TEST_SUITE_P(SchedulingModes, ProcessTaskTerminationTest, ::testing::Bool());
INSTANTIATE_TEST_SUITE_P(SchedulingModes, ProcessPartialOutputTerminationTest, ::testing::Bool());

}  // namespace jogasaki::testing
