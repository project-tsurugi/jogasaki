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

#include <jogasaki/utils/interference_size.h>

namespace jogasaki {

namespace scheduler {

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
     *
     * @brief accessor for request unique id
     * @return id value
     */
    [[nodiscard]] std::size_t id() const noexcept {
        return id_;
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

    static inline std::atomic_size_t id_src_{0};
};

} // namespace scheduler

} // namespace jogasaki