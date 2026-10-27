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
#include "session.h"

#include <cerrno>
#include <cstdlib>
#include <iomanip>
#include <set>
#include <sstream>
#include <utility>

#include <decimal.hh>
#include <takatori/datetime/conversion.h>
#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <tateyama/api/server/mock/request_response.h>

#include <jogasaki/proto/sql/error.pb.h>
#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/command_utils.h>
#include <jogasaki/utils/msgbuf_utils.h>

#include "result_formatter.h"
#include "text_utils.h"

namespace tateyama::service_cli {

namespace sql = jogasaki::proto::sql;
using jogasaki::common_cli::call_result;
using ValueCase = sql::request::Parameter::ValueCase;

namespace {

std::string_view commit_status_name(sql::request::CommitStatus st) {
    switch(st) {
        case sql::request::CommitStatus::ACCEPTED: return "ACCEPTED";
        case sql::request::CommitStatus::AVAILABLE: return "AVAILABLE";
        case sql::request::CommitStatus::STORED: return "STORED";
        case sql::request::CommitStatus::PROPAGATED: return "PROPAGATED";
        default: return "DEFAULT";
    }
}

std::string_view transaction_kind_name(jogasaki::common_cli::begin_options::kind k) {
    switch(k) {
        case jogasaki::common_cli::begin_options::kind::occ: return "OCC";
        case jogasaki::common_cli::begin_options::kind::ltx: return "LTX";
        case jogasaki::common_cli::begin_options::kind::rtx: return "RTX";
    }
    return "";
}

// convert parameter text into the value of the given type. Returns error message on failure.
std::string make_parameter(
    std::string const& name,
    std::string const& value,
    sql::common::AtomType type,
    std::vector<jogasaki::utils::parameter>& out
) {
    auto invalid = [&](std::string_view detail = {}) {
        std::string msg{"invalid value for "};
        msg.append(sql::common::AtomType_Name(type)).append(" parameter '").append(name).append("': ").append(value);
        if (! detail.empty()) {
            msg.append(" (").append(detail).append(")");
        }
        return msg;
    };
    auto parse_integer = [&](std::int64_t& v) {
        char* e{};
        errno = 0;
        v = std::strtoll(value.c_str(), &e, 10);
        return e != value.c_str() && *e == '\0' && errno == 0;
    };
    auto parse_float = [&](double& v) {
        char* e{};
        errno = 0;
        v = std::strtod(value.c_str(), &e);
        return e != value.c_str() && *e == '\0' && errno == 0;
    };
    switch(type) {
        case sql::common::AtomType::BOOLEAN: {
            auto v = to_upper(value);
            if (v != "TRUE" && v != "FALSE") {
                return invalid();
            }
            out.emplace_back(name, ValueCase::kBooleanValue, static_cast<std::int8_t>(v == "TRUE" ? 1 : 0));
            return {};
        }
        case sql::common::AtomType::INT4: {
            std::int64_t v{};
            if (! parse_integer(v)) {
                return invalid();
            }
            out.emplace_back(name, ValueCase::kInt4Value, static_cast<std::int32_t>(v));
            return {};
        }
        case sql::common::AtomType::INT8: {
            std::int64_t v{};
            if (! parse_integer(v)) {
                return invalid();
            }
            out.emplace_back(name, ValueCase::kInt8Value, v);
            return {};
        }
        case sql::common::AtomType::FLOAT4: {
            double v{};
            if (! parse_float(v)) {
                return invalid();
            }
            out.emplace_back(name, ValueCase::kFloat4Value, static_cast<float>(v));
            return {};
        }
        case sql::common::AtomType::FLOAT8: {
            double v{};
            if (! parse_float(v)) {
                return invalid();
            }
            out.emplace_back(name, ValueCase::kFloat8Value, v);
            return {};
        }
        case sql::common::AtomType::DECIMAL: {
            try {
                decimal::Decimal d{value};
                if (! d.isfinite()) {
                    return invalid();
                }
                out.emplace_back(name, ValueCase::kDecimalValue, takatori::decimal::triple{d.as_uint128_triple()});
            } catch (std::exception const& e) {
                return invalid(e.what());
            }
            return {};
        }
        case sql::common::AtomType::CHARACTER:
            out.emplace_back(name, ValueCase::kCharacterValue, value);
            return {};
        case sql::common::AtomType::OCTET: {
            // hex string
            if (value.size() % 2 != 0) {
                return invalid("hex string is expected");
            }
            std::string bin{};
            for(std::size_t i = 0; i < value.size(); i += 2) {
                char* e{};
                auto byte = std::strtol(value.substr(i, 2).c_str(), &e, 16);
                if (*e != '\0') {
                    return invalid("hex string is expected");
                }
                bin.push_back(static_cast<char>(byte));
            }
            out.emplace_back(name, ValueCase::kOctetValue, bin);
            return {};
        }
        case sql::common::AtomType::DATE: {
            auto res = takatori::datetime::parse_date(value);
            if (res.is_error()) {
                return invalid(res.error());
            }
            auto& v = res.value();
            out.emplace_back(name, ValueCase::kDateValue, takatori::datetime::date{static_cast<std::int32_t>(v.year), v.month, v.day});
            return {};
        }
        case sql::common::AtomType::TIME_OF_DAY: {
            auto res = takatori::datetime::parse_time(value);
            if (res.is_error()) {
                return invalid(res.error());
            }
            auto& v = res.value();
            out.emplace_back(name, ValueCase::kTimeOfDayValue, takatori::datetime::time_of_day{v.hour, v.minute, v.second, v.subsecond});
            return {};
        }
        case sql::common::AtomType::TIME_POINT: {
            auto res = takatori::datetime::parse_datetime(value);
            if (res.is_error()) {
                return invalid(res.error());
            }
            auto& v = res.value();
            if (v.offset.has_value()) {
                return invalid("time zone offset is not supported");
            }
            out.emplace_back(name, ValueCase::kTimePointValue, takatori::datetime::time_point{
                takatori::datetime::date{static_cast<std::int32_t>(v.date.year), v.date.month, v.date.day},
                takatori::datetime::time_of_day{v.time.hour, v.time.minute, v.time.second, v.time.subsecond}
            });
            return {};
        }
        default:
            return invalid("unsupported type");
    }
}

std::string elapsed_ms(std::chrono::steady_clock::duration d) {
    std::ostringstream ss{};
    ss << std::fixed << std::setprecision(3) << std::chrono::duration<double, std::milli>(d).count();
    return ss.str();
}

}  // namespace

session::session(
    std::string name,
    std::size_t session_id,
    std::shared_ptr<jogasaki::api::impl::service> service,
    session_settings& settings,
    output& out
) :
    name_(std::move(name)),
    session_id_(session_id),
    client_(std::move(service), session_id, {jogasaki::common_cli::wait_kind::sleep, {}}),
    settings_(settings),
    out_(out),
    thread_([this]() { run(); })
{}

session::~session() {
    close(false);
}

void session::submit_statement(std::string text) {
    {
        std::lock_guard lk{mutex_};
        queue_.push_back({task::kind::statement, std::move(text)});
    }
    cv_.notify_all();
}

void session::submit_expect_error() {
    {
        std::lock_guard lk{mutex_};
        queue_.push_back({task::kind::expect_error, {}});
    }
    cv_.notify_all();
}

bool session::idle() const {
    std::lock_guard lk{mutex_};
    return queue_.empty() && ! running_task_;
}

bool session::wait_idle(std::chrono::milliseconds timeout) {
    std::unique_lock lk{mutex_};
    auto pred = [&]() { return queue_.empty() && ! running_task_; };
    if (timeout.count() == 0) {
        cv_.wait(lk, pred);
        return true;
    }
    return cv_.wait_for(lk, timeout, pred);
}

std::string session::status() const {
    std::ostringstream ss{};
    std::lock_guard lk{mutex_};
    ss << "session_id=" << session_id_;
    if (tx_active_) {
        ss << " transaction=" << tx_id_ << (tx_implicit_ ? "(implicit)" : "");
    } else {
        ss << " transaction=none";
    }
    if (running_task_) {
        ss << " running=" << elapsed_ms(std::chrono::steady_clock::now() - current_begin_) << "ms '" << current_text_ << "'";
    } else {
        ss << " idle";
    }
    if (! queue_.empty()) {
        ss << " queued=" << queue_.size();
    }
    return ss.str();
}

void session::close(bool cleanup) {
    {
        std::lock_guard lk{mutex_};
        if (! thread_.joinable()) {
            return;
        }
        if (cleanup) {
            queue_.push_back({task::kind::cleanup, {}});
        }
        stop_ = true;
    }
    cv_.notify_all();
    thread_.join();
}

void session::run() {
    while(true) {
        task t{};
        {
            std::unique_lock lk{mutex_};
            cv_.wait(lk, [&]() { return ! queue_.empty() || stop_; });
            if (queue_.empty()) {
                return;
            }
            t = std::move(queue_.front());
            queue_.pop_front();
            running_task_ = true;
            current_text_ = t.text_;
            current_begin_ = std::chrono::steady_clock::now();
        }
        execute(t);
        {
            std::lock_guard lk{mutex_};
            running_task_ = false;
            current_text_.clear();
        }
        cv_.notify_all();
    }
}

void session::execute(task const& t) {
    std::vector<std::string> lines{};
    switch(t.kind_) {
        case task::kind::expect_error:
            expect_error_ = true;
            return;
        case task::kind::cleanup:
            cleanup(lines);
            break;
        case task::kind::statement:
            execute_statement(t.text_, lines);
            break;
    }
    if (! lines.empty()) {
        out_.print(name_, lines);
    }
}

void session::execute_statement(std::string const& text, std::vector<std::string>& lines) {
    if (settings_.echo_) {
        // print immediately so that the timeline shows when the statement started
        out_.print(name_, "> " + text + (starts_with(text, "\\") && ! starts_with(text, "\\prepare") && ! starts_with(text, "\\execute") ? "" : ";"));
    }
    auto begin = std::chrono::steady_clock::now();
    statement stmt{};
    bool success{};
    if (auto err = parse_statement(text, stmt); ! err.empty()) {
        lines.emplace_back("error: " + err);
    } else {
        switch(stmt.kind_) {
            case statement::kind::begin: success = do_begin(stmt, lines); break;
            case statement::kind::commit: success = do_commit(stmt, lines); break;
            case statement::kind::rollback: success = do_rollback(lines); break;
            case statement::kind::sql: success = do_sql(stmt, lines); break;
            case statement::kind::explain: success = do_explain(stmt, lines); break;
            case statement::kind::prepare: success = do_prepare(stmt, lines); break;
            case statement::kind::execute: success = do_execute(stmt, lines); break;
            case statement::kind::special:
                if (stmt.name_ == "show") {
                    success = do_show(stmt, lines);
                } else {
                    lines.emplace_back("error: unsupported command: \\" + stmt.name_);
                }
                break;
            case statement::kind::call:
                lines.emplace_back("error: CALL statement is not supported");
                break;
        }
    }
    if (settings_.timing_) {
        lines.emplace_back("Time: " + elapsed_ms(std::chrono::steady_clock::now() - begin) + " ms");
    }
    if (expect_error_) {
        expect_error_ = false;
        if (success) {
            lines.emplace_back("error: expected error did not occur");
            ++settings_.errors_;
        } else {
            lines.emplace_back("(error is expected)");
        }
        return;
    }
    if (! success) {
        ++settings_.errors_;
    }
}

void session::report_error(call_result const& res, std::vector<std::string>& lines) const {
    auto& diag = res.diag();
    lines.emplace_back("error: " + sql::error::Code_Name(diag.code_) + ": " + diag.message_);
    if (! diag.supplemental_text_.empty()) {
        lines.emplace_back("  supplemental text: " + diag.supplemental_text_);
    }
}

bool session::do_begin(statement const& stmt, std::vector<std::string>& lines) {
    if (tx_active_) {
        lines.emplace_back("error: transaction is already started: " + tx_id_);
        return false;
    }
    jogasaki::api::transaction_handle tx{};
    std::string id{};
    if (auto res = client_.begin(stmt.begin_, tx, &id); ! res) {
        report_error(res, lines);
        return false;
    }
    {
        std::lock_guard lk{mutex_};
        tx_ = tx;
        tx_id_ = id;
        tx_implicit_ = false;
        tx_active_ = true;
    }
    lines.emplace_back("transaction started. type=" + std::string{transaction_kind_name(stmt.begin_.kind_)} + " transaction_id=" + id);
    return true;
}

bool session::end_transaction(bool commit, std::vector<std::string>& lines, std::string_view suffix) {
    call_result res{};
    if (commit) {
        res = client_.commit(tx_);
    } else {
        res = client_.rollback(tx_);
    }
    {
        std::lock_guard lk{mutex_};
        tx_active_ = false;
        tx_ = {};
    }
    if (! res) {
        report_error(res, lines);
        return false;
    }
    if (suffix.empty() || settings_.display_implicit_) {
        // non-empty suffix means the transaction is ended implicitly
        lines.emplace_back(std::string{commit ? "transaction commit finished" : "transaction rollback finished"} + std::string{suffix} + ".");
    }
    return true;
}

bool session::do_commit(statement const& stmt, std::vector<std::string>& lines) {
    if (! tx_active_) {
        lines.emplace_back("commit request is ignored because transaction is not active");
        return true;
    }
    auto res = client_.commit(tx_, stmt.commit_status_);
    {
        std::lock_guard lk{mutex_};
        tx_active_ = false;
        tx_ = {};
    }
    if (! res) {
        report_error(res, lines);
        return false;
    }
    lines.emplace_back("transaction commit(" + std::string{commit_status_name(stmt.commit_status_)} + ") finished.");
    return true;
}

bool session::do_rollback(std::vector<std::string>& lines) {
    if (! tx_active_) {
        lines.emplace_back("rollback request is ignored because transaction is not active");
        return true;
    }
    return end_transaction(false, lines, "");
}

bool session::execute_prepared(
    std::vector<std::string>& lines,
    std::uint64_t stmt,
    bool query,
    std::vector<jogasaki::utils::parameter> const& parameters
) {
    if (! query) {
        std::shared_ptr<jogasaki::request_statistics> stats{};
        if (auto res = client_.execute_prepared_statement(tx_, stmt, parameters, &stats); ! res) {
            report_error(res, lines);
            return false;
        }
        lines.emplace_back(format_statement_result(stats));
        return true;
    }
    std::mutex data_mutex{};
    std::string data{};
    jogasaki::common_cli::query_info info{};
    auto res = client_.execute_prepared_query(tx_, stmt, parameters, [&](std::string_view d) {
        std::lock_guard lk{data_mutex};
        data.append(d);
    }, &info);
    if (! res) {
        report_error(res, lines);
        return false;
    }
    lines.emplace_back(format_header(info.columns_));
    if (settings_.raw_) {
        std::ostringstream ss{};
        ss << "raw: " << jogasaki::utils::binary_printer{data.data(), data.size()};
        lines.emplace_back(ss.str());
    }
    auto meta = jogasaki::utils::create_record_meta(info.columns_);
    auto records = jogasaki::utils::deserialize_msg(std::string_view{data}, meta);
    for(auto&& r : records) {
        lines.emplace_back(format_record(r));
    }
    lines.emplace_back(format_row_count(records.size()));
    return true;
}

bool session::run_in_transaction(
    std::vector<std::string>& lines,
    std::uint64_t stmt,
    bool query,
    std::vector<jogasaki::utils::parameter> const& parameters
) {
    bool implicit = false;
    if (! tx_active_) {
        if (settings_.manual_) {
            lines.emplace_back("error: transaction is not started");
            return false;
        }
        jogasaki::api::transaction_handle tx{};
        std::string id{};
        if (auto res = client_.begin(settings_.implicit_options_, tx, &id); ! res) {
            report_error(res, lines);
            return false;
        }
        {
            std::lock_guard lk{mutex_};
            tx_ = tx;
            tx_id_ = id;
            tx_implicit_ = true;
            tx_active_ = true;
        }
        if (settings_.display_implicit_) {
            lines.emplace_back("start transaction implicitly. type=" +
                std::string{transaction_kind_name(settings_.implicit_options_.kind_)} + " transaction_id=" + id);
        }
        implicit = true;
    }
    bool success = execute_prepared(lines, stmt, query, parameters);
    if (implicit) {
        return end_transaction(success, lines, " implicitly") && success;
    }
    if (success && settings_.auto_commit_) {
        return end_transaction(true, lines, " (auto commit)");
    }
    return success;
}

bool session::do_sql(statement const& stmt, std::vector<std::string>& lines) {
    std::uint64_t handle{};
    bool query{};
    if (auto res = client_.prepare(stmt.text_, {}, handle, &query); ! res) {
        report_error(res, lines);
        return false;
    }
    auto success = run_in_transaction(lines, handle, query, {});
    (void) client_.dispose_statement(handle);
    return success;
}

bool session::do_explain(statement const& stmt, std::vector<std::string>& lines) {
    std::string out{};
    if (auto res = client_.explain(stmt.sql_, out); ! res) {
        report_error(res, lines);
        return false;
    }
    lines.emplace_back(out);
    return true;
}

bool session::do_prepare(statement const& stmt, std::vector<std::string>& lines) {
    std::unordered_map<std::string, sql::common::AtomType> placeholders{};
    prepared p{};
    for(auto&& [n, t] : stmt.variables_) {
        placeholders.emplace(n, t);
        p.variables_.emplace(n, t);
    }
    if (auto res = client_.prepare(stmt.sql_, placeholders, p.handle_, &p.has_result_records_); ! res) {
        report_error(res, lines);
        return false;
    }
    if (auto it = prepared_.find(stmt.name_); it != prepared_.end()) {
        (void) client_.dispose_statement(it->second.handle_);
        prepared_.erase(it);
    }
    prepared_.emplace(stmt.name_, std::move(p));
    lines.emplace_back("statement " + stmt.name_ + " prepared.");
    return true;
}

bool session::do_execute(statement const& stmt, std::vector<std::string>& lines) {
    auto it = prepared_.find(stmt.name_);
    if (it == prepared_.end()) {
        lines.emplace_back("error: prepared statement not found: " + stmt.name_);
        return false;
    }
    auto& p = it->second;
    std::vector<jogasaki::utils::parameter> parameters{};
    std::set<std::string> assigned{};
    for(auto&& [name, value] : stmt.parameters_) {
        auto v = p.variables_.find(name);
        if (v == p.variables_.end()) {
            lines.emplace_back("error: variable is not defined in the prepared statement: " + name);
            return false;
        }
        assigned.emplace(name);
        if (! value.has_value()) {
            parameters.emplace_back(name);
            continue;
        }
        if (auto err = make_parameter(name, *value, v->second, parameters); ! err.empty()) {
            lines.emplace_back("error: " + err);
            return false;
        }
    }
    for(auto&& [name, type] : p.variables_) {
        if (assigned.count(name) == 0) {
            lines.emplace_back("error: value is not assigned for variable: " + name);
            return false;
        }
    }
    return run_in_transaction(lines, p.handle_, p.has_result_records_, parameters);
}

bool session::do_show(statement const& stmt, std::vector<std::string>& lines) {
    if (stmt.arguments_.empty() || to_lower(stmt.arguments_[0]) != "table") {
        lines.emplace_back("error: usage: \\show table [<table name>]");
        return false;
    }
    if (stmt.arguments_.size() == 1) {
        std::vector<std::string> tables{};
        if (auto res = client_.list_tables(tables); ! res) {
            report_error(res, lines);
            return false;
        }
        for(auto&& t : tables) {
            lines.emplace_back(t);
        }
        return true;
    }
    jogasaki::executor::dto::describe_table dt{};
    if (auto res = client_.describe_table(stmt.arguments_[1], dt); ! res) {
        report_error(res, lines);
        return false;
    }
    lines.emplace_back(dt.table_name_ + " (");
    for(auto&& c : dt.columns_) {
        lines.emplace_back("    " + c.name_ + ": " + format_type(c) + (c.nullable_.has_value() && ! *c.nullable_ ? " NOT NULL" : ""));
    }
    std::string pk{};
    for(auto&& k : dt.primary_key_) {
        pk.append(pk.empty() ? "" : ", ").append(k);
    }
    lines.emplace_back(")" + (pk.empty() ? std::string{} : " PRIMARY KEY(" + pk + ")"));
    return true;
}

void session::cleanup(std::vector<std::string>& lines) {
    if (tx_active_) {
        end_transaction(false, lines, " on exit");
    }
    for(auto&& [name, p] : prepared_) {
        (void) client_.dispose_statement(p.handle_);
    }
    prepared_.clear();
}

}  // namespace tateyama::service_cli
