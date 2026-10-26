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
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <jogasaki/api/impl/service.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/proto/sql/common.pb.h>

#include "output.h"
#include "service_client.h"
#include "statement.h"

namespace tateyama::service_cli {

/**
 * @brief settings shared by sessions
 */
struct session_settings {
    /// @brief options for transactions started implicitly
    jogasaki::common_cli::begin_options implicit_options_{};  //NOLINT

    /// @brief whether transactions are never started implicitly
    bool manual_{};  //NOLINT

    /// @brief whether to commit after each statement even in explicitly started transaction
    bool auto_commit_{};  //NOLINT

    /// @brief whether to echo the statement before executing
    bool echo_{};  //NOLINT

    /// @brief whether to print execution time
    std::atomic_bool timing_{};  //NOLINT

    /// @brief whether to print the raw result set data
    bool raw_{};  //NOLINT

    /// @brief the number of unexpected errors
    std::atomic<std::size_t> errors_{};  //NOLINT
};

/**
 * @brief session executing statements on its own thread
 * @details statements submitted to a session are executed in order, while different sessions run concurrently.
 * Each session has its own session id, transaction and prepared statements.
 */
class session {
public:
    session(
        std::string name,
        std::size_t session_id,
        std::shared_ptr<jogasaki::api::impl::service> service,
        session_settings& settings,
        output& out
    );

    ~session();

    session(session const&) = delete;
    session& operator=(session const&) = delete;
    session(session&&) = delete;
    session& operator=(session&&) = delete;

    /**
     * @brief submit statement text to execute
     */
    void submit_statement(std::string text);

    /**
     * @brief submit expectation that the next statement fails
     */
    void submit_expect_error();

    /**
     * @brief returns whether no statement is running or queued
     */
    [[nodiscard]] bool idle() const;

    /**
     * @brief wait until the session becomes idle
     * @param timeout the timeout duration. Zero means waiting forever.
     * @return false if timed out
     */
    bool wait_idle(std::chrono::milliseconds timeout);

    /**
     * @brief returns the status text of this session
     */
    [[nodiscard]] std::string status() const;

    /**
     * @brief stop the session thread
     * @param cleanup whether to rollback the transaction and dispose prepared statements before stopping
     */
    void close(bool cleanup);

    [[nodiscard]] std::string const& name() const noexcept {
        return name_;
    }

private:
    struct task {
        enum class kind {
            statement,
            expect_error,
            cleanup,
        };
        kind kind_{};
        std::string text_{};
    };

    struct prepared {
        std::uint64_t handle_{};
        bool has_result_records_{};
        std::map<std::string, jogasaki::proto::sql::common::AtomType> variables_{};
    };

    std::string name_{};
    std::size_t session_id_{};
    jogasaki::common_cli::service_client client_{};
    session_settings& settings_;
    output& out_;

    mutable std::mutex mutex_{};
    std::condition_variable cv_{};
    std::deque<task> queue_{};
    bool running_task_{};
    bool stop_{};
    std::string current_text_{};
    std::chrono::steady_clock::time_point current_begin_{};

    // states accessed only from the session thread (and status() for display)
    jogasaki::api::transaction_handle tx_{};
    std::atomic_bool tx_active_{};
    bool tx_implicit_{};
    std::string tx_id_{};
    bool expect_error_{};
    std::map<std::string, prepared> prepared_{};

    // thread must come last since it starts running with the members above
    std::thread thread_{};

    void run();
    void execute(task const& t);
    void execute_statement(std::string const& text, std::vector<std::string>& lines);
    void cleanup(std::vector<std::string>& lines);

    bool do_begin(statement const& stmt, std::vector<std::string>& lines);
    bool do_commit(statement const& stmt, std::vector<std::string>& lines);
    bool do_rollback(std::vector<std::string>& lines);
    bool do_sql(statement const& stmt, std::vector<std::string>& lines);
    bool do_explain(statement const& stmt, std::vector<std::string>& lines);
    bool do_prepare(statement const& stmt, std::vector<std::string>& lines);
    bool do_execute(statement const& stmt, std::vector<std::string>& lines);
    bool do_show(statement const& stmt, std::vector<std::string>& lines);

    bool run_in_transaction(
        std::vector<std::string>& lines,
        std::uint64_t stmt,
        bool query,
        std::vector<jogasaki::utils::parameter> const& parameters
    );
    bool execute_prepared(
        std::vector<std::string>& lines,
        std::uint64_t stmt,
        bool query,
        std::vector<jogasaki::utils::parameter> const& parameters
    );
    bool end_transaction(bool commit, std::vector<std::string>& lines, std::string_view suffix);
    void report_error(jogasaki::common_cli::call_result const& res, std::vector<std::string>& lines) const;
};

}  // namespace tateyama::service_cli
