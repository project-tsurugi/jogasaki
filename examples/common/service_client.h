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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <tateyama/api/server/mock/request_response.h>

#include <jogasaki/api/impl/service.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/executor/dto/common_column.h>
#include <jogasaki/executor/dto/describe_table.h>
#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/proto/sql/error.pb.h>
#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/utils/command_utils.h>

namespace jogasaki::common_cli {

/**
 * @brief how the client waits for the response completion
 */
enum class wait_kind {
    /// @brief busy loop - lowest latency, occupies the core while waiting
    spin,
    /// @brief poll with sleep - releases the core while waiting
    sleep,
};

/**
 * @brief policy for waiting the response completion
 */
struct wait_policy {
    wait_kind kind_{wait_kind::sleep};  //NOLINT

    /// @brief timeout duration. Zero means waiting forever.
    std::chrono::milliseconds timeout_{};  //NOLINT
};

/**
 * @brief error information returned from the server
 * @details strings are filled only on failure
 */
struct diagnostic {
    /// @brief the error code returned by the sql service (UNKNOWN if the error is not from the sql service)
    proto::sql::error::Code code_{};  //NOLINT

    std::string message_{};  //NOLINT

    std::string supplemental_text_{};  //NOLINT
};

/**
 * @brief result of the service call
 */
class call_result {
public:
    call_result() = default;

    explicit call_result(diagnostic diag) :
        ok_(false),
        diagnostic_(std::move(diag))
    {}

    [[nodiscard]] bool ok() const noexcept {
        return ok_;
    }

    explicit operator bool() const noexcept {
        return ok_;
    }

    [[nodiscard]] diagnostic const& diag() const noexcept {
        return diagnostic_;
    }

private:
    bool ok_{true};
    diagnostic diagnostic_{};
};

/**
 * @brief transaction options for begin
 */
struct begin_options {
    enum class kind {
        occ,
        ltx,
        rtx,
    };
    kind kind_{kind::occ};  //NOLINT
    std::vector<std::string> write_preserves_{};  //NOLINT
    std::vector<std::string> inclusive_read_areas_{};  //NOLINT
    std::vector<std::string> exclusive_read_areas_{};  //NOLINT
    proto::sql::request::TransactionPriority priority_{};  //NOLINT
    std::string label_{};  //NOLINT
    bool modifies_definitions_{};  //NOLINT
};

/**
 * @brief result set information of the query
 */
struct query_info {
    std::string name_{};  //NOLINT
    std::vector<executor::dto::common_column> columns_{};  //NOLINT
};

/**
 * @brief callback receiving result set data written by the server
 * @details this can be called concurrently from multiple writers on the server side
 */
using data_handler = std::function<void(std::string_view)>;

/**
 * @brief thin client calling sql service api in-process
 * @details this object sends requests on behalf of a single session. This object doesn't print messages,
 * doesn't create threads, and doesn't manage transactions or statements. Callers are responsible for those.
 * Member functions are not thread-safe - use separate objects for concurrent calls.
 */
class service_client {
public:
    service_client() = default;

    /**
     * @brief create new object
     * @param service the sql service to call
     * @param session_id the session id attached to requests. Zero means requests without session.
     * @param policy the policy to wait for responses
     */
    service_client(
        std::shared_ptr<api::impl::service> service,
        std::size_t session_id,
        wait_policy policy
    ) noexcept;

    [[nodiscard]] std::size_t session_id() const noexcept {
        return session_id_;
    }

    /**
     * @brief begin transaction
     * @param out filled with the transaction handle
     * @param transaction_id if non-null, filled with the transaction id
     */
    call_result begin(begin_options const& options, api::transaction_handle& out, std::string* transaction_id = nullptr);

    /**
     * @brief commit transaction
     * @param status the commit status to wait for. Unspecified means the default one.
     */
    call_result commit(
        api::transaction_handle tx,
        proto::sql::request::CommitStatus status = proto::sql::request::CommitStatus::COMMIT_STATUS_UNSPECIFIED
    );

    call_result rollback(api::transaction_handle tx);

    /**
     * @brief prepare statement
     * @param sql the sql text
     * @param placeholders the placeholder names and types
     * @param out filled with the prepared statement handle
     * @param has_result_records if non-null, filled with whether the statement is a query
     */
    call_result prepare(
        std::string_view sql,
        std::unordered_map<std::string, proto::sql::common::AtomType> const& placeholders,
        std::uint64_t& out,
        bool* has_result_records = nullptr
    );

    call_result dispose_statement(std::uint64_t stmt);

    /**
     * @brief execute statement (non-query)
     * @param stats if non-null, filled with the execution statistics
     */
    call_result execute_statement(
        api::transaction_handle tx,
        std::string_view sql,
        std::shared_ptr<request_statistics>* stats = nullptr
    );

    call_result execute_prepared_statement(
        api::transaction_handle tx,
        std::uint64_t stmt,
        std::vector<utils::parameter> const& parameters,
        std::shared_ptr<request_statistics>* stats = nullptr
    );

    /**
     * @brief execute query
     * @param on_data the callback receiving result set data
     * @param info if non-null, filled with the result set information
     */
    call_result execute_query(
        api::transaction_handle tx,
        std::string_view sql,
        data_handler on_data,
        query_info* info = nullptr
    );

    call_result execute_prepared_query(
        api::transaction_handle tx,
        std::uint64_t stmt,
        std::vector<utils::parameter> const& parameters,
        data_handler on_data,
        query_info* info = nullptr
    );

    /**
     * @brief explain the prepared statement
     * @param out filled with the explained text (json)
     */
    call_result explain(
        std::uint64_t stmt,
        std::vector<utils::parameter> const& parameters,
        std::string& out
    );

    /**
     * @brief explain the sql text
     * @param out filled with the explained text (json)
     */
    call_result explain(std::string_view sql, std::string& out);

    /**
     * @brief list table names
     */
    call_result list_tables(std::vector<std::string>& out);

    /**
     * @brief describe the table
     */
    call_result describe_table(std::string_view name, executor::dto::describe_table& out);

private:
    std::shared_ptr<api::impl::service> service_{};
    std::size_t session_id_{};
    wait_policy policy_{};

    /**
     * @brief send request and wait for its completion
     * @return error result if the call failed in the service framework, or the response timed out
     */
    call_result send(
        std::string_view payload,
        std::shared_ptr<tateyama::api::server::mock::test_response>& res,
        data_handler on_data = {}
    );

    call_result execute_query_common(
        std::string_view payload,
        data_handler on_data,
        query_info* info
    );
};

}  // namespace jogasaki::common_cli
