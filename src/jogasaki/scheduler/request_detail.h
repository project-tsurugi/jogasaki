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

#include <atomic>
#include <cstdlib>
#include <deque>
#include <iosfwd>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::scheduler {

enum class request_detail_kind {
    unknown,
    prepare,
    begin,
    commit,
    rollback,
    dispose_statement,
    execute_statement,
    dump,
    load,
    explain,
    describe_table,
    batch,

    // internal
    process_durability_callback,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(request_detail_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = request_detail_kind;
    switch (value) {
        case kind::unknown: return "unknown"sv;
        case kind::prepare: return "prepare"sv;
        case kind::begin: return "begin"sv;
        case kind::commit: return "commit"sv;
        case kind::rollback: return "rollback"sv;
        case kind::execute_statement: return "execute_statement"sv;
        case kind::dispose_statement: return "dispose_statement"sv;
        case kind::dump: return "dump"sv;
        case kind::load: return "load"sv;
        case kind::explain: return "explain"sv;
        case kind::describe_table: return "describe_table"sv;
        case kind::batch: return "batch"sv;
        case kind::process_durability_callback: return "process_durability_callback"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, request_detail_kind value) {
    return out << to_string_view(value);
}

/**
 * @brief job status for the diagnostics info
 */
enum class request_detail_status {
    //@brief undefined status
    undefined,

    //@brief request has been accepted by the sql engine
    accepted,

    //@brief the job requires compiling statement (prepare and creating executable statement) and the compiling task is running
    compiling,

    //@brief one of the tasks submitted to the scheduler and placed on the queue
    submitted,

    //@brief one of the tasks for the job has started running
    executing,

    //@brief async request has been made to cc and waiting its completion
    waiting_cc,

    //@brief all tasks for the job except tear-down have been completed
    completing,

    //@brief job tear-down is going to finish
    finishing,

    //@brief the job is being canceled due to error in execution
    canceling,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(request_detail_status value) noexcept {
    using namespace std::string_view_literals;
    using status = request_detail_status;
    switch (value) {
        case status::undefined: return "undefined"sv;
        case status::accepted: return "accepted"sv;
        case status::compiling: return "compiling"sv;
        case status::submitted: return "submitted"sv;
        case status::executing: return "executing"sv;
        case status::waiting_cc: return "waiting_cc"sv;
        case status::completing: return "completing"sv;
        case status::finishing: return "finishing"sv;
        case status::canceling: return "canceling"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, request_detail_status value) {
    return out << to_string_view(value);
}

enum class request_detail_channel_status {
    //@brief undefined status
    undefined,

    //@brief channel has been acquired
    acquired,

    //@brief one of the writers from the channel has been used to write output data
    transferring,

    //@brief channel has been released
    released,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(request_detail_channel_status value) noexcept {
    using namespace std::string_view_literals;
    using status = request_detail_channel_status;
    switch (value) {
        case status::undefined: return "undefined"sv;
        case status::acquired: return "acquired"sv;
        case status::transferring: return "transferring"sv;
        case status::released: return "released"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, request_detail_channel_status value) {
    return out << to_string_view(value);
}

class affected_transactions {
public:
    /**
     * @brief create default object
     */
    affected_transactions() = default;

    ~affected_transactions() = default;
    affected_transactions(affected_transactions const& other) = default;
    affected_transactions& operator=(affected_transactions const& other) = default;
    affected_transactions(affected_transactions&& other) noexcept = default;
    affected_transactions& operator=(affected_transactions&& other) noexcept = default;

    void add(std::string_view tx_id) noexcept {
        tx_ids_.emplace_back(tx_id);
    }

    auto begin() noexcept {
        return tx_ids_.begin();
    }

    auto end() noexcept {
        return tx_ids_.end();
    }

    [[nodiscard]] auto begin() const noexcept {
        return tx_ids_.begin();
    }

    [[nodiscard]] auto end() const noexcept {
        return tx_ids_.end();
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return tx_ids_.size();
    }

    [[nodiscard]] bool empty() const noexcept {
        return tx_ids_.empty();
    }

    void clear() {
        tx_ids_.clear();
    }

private:
    std::deque<std::string> tx_ids_{};
};

inline std::ostream& operator<<(std::ostream& out, affected_transactions const& value) {
    bool first = true;
    for(auto&& tx_id : value) {
        if(! first) {
            out << ",";
        }
        first = false;
        out << tx_id;
    }
    return out;
}

/**
 * @brief diagnostics info object for the job
 * @details this class represents detailed job information in the context of sql request
 * This is separated from job_context, which is purely job scheduler construct,
 * while diagnostics is in the context of sql request from client.
 */
class cache_align request_detail {
public:
    /**
     * @brief create default context object
     */
    request_detail() = default;

    ~request_detail() = default;
    request_detail(request_detail const& other) = delete;
    request_detail& operator=(request_detail const& other) = delete;
    request_detail(request_detail&& other) noexcept = delete;
    request_detail& operator=(request_detail&& other) noexcept = delete;

    explicit request_detail(request_detail_kind arg) :
        kind_(arg)
    {}

    void kind(request_detail_kind arg) noexcept {
        kind_ = arg;
    }

    [[nodiscard]] request_detail_kind kind() const noexcept {
        return kind_;
    }

    void transaction_id(std::string_view arg) noexcept {
        transaction_id_ = arg;
    }

    [[nodiscard]] std::string_view transaction_id() const noexcept {
        return transaction_id_;
    }

    void statement_text(std::shared_ptr<std::string> sql) noexcept {
        statement_text_ = std::move(sql);
    }

    [[nodiscard]] std::string_view statement_text() const noexcept {
        if(! statement_text_) return {};
        return *statement_text_;
    }

    void channel_name(std::string_view arg) noexcept {
        channel_name_ = arg;
    }

    [[nodiscard]] std::string_view channel_name() const noexcept {
        return channel_name_;
    }

    void status(request_detail_status st) {
        status_ = st;
    }

    [[nodiscard]] request_detail_status status() const noexcept {
        return status_;
    }

    void channel_status(request_detail_channel_status st) {
        channel_status_ = st;
    }

    [[nodiscard]] request_detail_channel_status channel_status() const noexcept {
        return channel_status_;
    }

    void transaction_option_spec(std::string_view arg) {
        transaction_option_spec_ = arg;
    }

    [[nodiscard]] std::string_view transaction_option_spec() const noexcept {
        return transaction_option_spec_;
    }
    /**
     * @brief accessor for request unique id
     * @return id value
     */
    [[nodiscard]] std::size_t id() const noexcept {
        return id_;
    }

    /**
     * @brief accessor for task duration field
     * @return task duration field reference
     */
    [[nodiscard]] std::atomic_size_t& task_duration_ns() noexcept {
        return task_duration_ns_;
    }

    /**
     * @brief accessor for task count field
     * @return task count field reference
     */
    [[nodiscard]] std::atomic_size_t& task_count() noexcept {
        return task_count_;
    }

    /**
     * @brief accessor for task stealing count field
     * @return task stealing count field reference
     */
    [[nodiscard]] std::atomic_size_t& task_steling_count() noexcept {
        return task_stealing_count_;
    }

    /**
     * @brief accessor for sticky task count field
     * @return sticky task count field reference
     */
    [[nodiscard]] std::atomic_size_t& sticky_task_count() noexcept {
        return sticky_task_count_;
    }

    /**
     * @brief accessor for sticky task reassigned count field
     * @return counter of the sticky task reassigned to different worker than default candidate
     */
    [[nodiscard]] std::atomic_size_t& sticky_task_worker_enforced_count() noexcept {
        return sticky_task_worker_enforced_count_;
    }

    /**
     * @brief setter of the hybrid_execution_mode
     */
    void hybrid_execution_mode(hybrid_execution_mode_kind arg) {
        hybrid_execution_mode_ = arg;
    }

    /**
     * @brief getter of the hybrid_execution_mode
     * @returns the mode (serial/stealing) on which the requested job has been run on. This is set undefined if
     * job is not scheduled/executed with hybrid scheduler.
     */
    [[nodiscard]] hybrid_execution_mode_kind hybrid_execution_mode() const noexcept {
        return hybrid_execution_mode_;
    }

    /**
     * @brief accessor for affected transactions
     * @return affected transactions object reference
     */
    [[nodiscard]] affected_transactions& affected_txs() noexcept {
        return affected_transactions_;
    }

    /**
     * @brief accessor for affected transactions
     * @return affected transactions object reference
     */
    [[nodiscard]] affected_transactions const& affected_txs() const noexcept {
        return affected_transactions_;
    }
private:
    std::size_t id_{id_src_++};
    request_detail_kind kind_{};
    std::string transaction_id_{};
    std::string channel_name_{};
    std::shared_ptr<std::string> statement_text_{};
    std::atomic<request_detail_status> status_{};
    std::atomic<request_detail_channel_status> channel_status_{};
    std::string transaction_option_spec_{};
    std::atomic_size_t task_duration_ns_{};
    std::atomic_size_t task_count_{};
    std::atomic_size_t task_stealing_count_{};
    std::atomic_size_t sticky_task_count_{};
    std::atomic_size_t sticky_task_worker_enforced_count_{};
    std::atomic<hybrid_execution_mode_kind> hybrid_execution_mode_{hybrid_execution_mode_kind::undefined};
    affected_transactions affected_transactions_{};

    cache_align static inline std::atomic_size_t id_src_{0}; //NOLINT
};

} // namespace jogasaki::scheduler
