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
#include "service_client.h"

#include <thread>
#include <utility>

#include <tateyama/api/server/mock/request_response.h>
#include <tateyama/proto/diagnostics.pb.h>

#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/proto/sql/response.pb.h>

namespace jogasaki::common_cli {

using namespace std::chrono_literals;
namespace sql = proto::sql;
using tateyama::api::server::mock::test_request;
using tateyama::api::server::mock::test_response;

namespace {

call_result error_result(sql::error::Code code, std::string_view message, std::string_view supplemental_text = {}) {
    return call_result{diagnostic{code, std::string{message}, std::string{supplemental_text}}};
}

template <class T>
call_result from_error(T const& err) {
    return error_result(err.code(), err.detail(), err.supplemental_text());
}

call_result missing_response(std::string_view kind) {
    std::string msg{"unexpected response - missing "};
    msg.append(kind);
    return error_result(sql::error::Code::CODE_UNSPECIFIED, msg);
}

bool parse_response(std::string_view body, sql::response::Response& out) {
    return out.ParseFromArray(body.data(), static_cast<int>(body.size()));
}

call_result decode_result_only(std::string_view body) {
    sql::response::Response resp{};
    if (! parse_response(body, resp) || ! resp.has_result_only()) {
        return missing_response("result_only");
    }
    auto& ro = resp.result_only();
    if (ro.has_error()) {
        return from_error(ro.error());
    }
    return {};
}

call_result decode_execute_result(std::string_view body, std::shared_ptr<request_statistics>* stats) {
    sql::response::Response resp{};
    if (! parse_response(body, resp) || ! resp.has_execute_result()) {
        return missing_response("execute_result");
    }
    auto& er = resp.execute_result();
    if (er.has_error()) {
        return from_error(er.error());
    }
    if (stats != nullptr) {
        *stats = utils::make_stats(er.success());
    }
    return {};
}

}  // namespace

service_client::service_client(
    std::shared_ptr<api::impl::service> service,
    std::size_t session_id,
    wait_policy policy
) noexcept :
    service_(std::move(service)),
    session_id_(session_id),
    policy_(policy)
{}

call_result service_client::send(
    std::string_view payload,
    std::shared_ptr<test_response>& res,
    data_handler on_data
) {
    auto req = std::make_shared<test_request>(payload, session_id_);
    res = std::make_shared<test_response>();
    if (on_data) {
        res->set_on_write(std::move(on_data));
    }
    if (! (*service_)(req, res)) {
        return error_result(sql::error::Code::CODE_UNSPECIFIED, "service invocation failed");
    }
    bool completed{};
    if (policy_.kind_ == wait_kind::spin) {
        if (policy_.timeout_.count() == 0) {
            while(! res->wait_completion(1h)) {}
            completed = true;
        } else {
            completed = res->wait_completion(policy_.timeout_);
        }
    } else {
        auto begin = std::chrono::steady_clock::now();
        while(true) {
            if (res->completed()) {
                completed = true;
                break;
            }
            if (policy_.timeout_.count() != 0 && std::chrono::steady_clock::now() - begin > policy_.timeout_) {
                break;
            }
            std::this_thread::sleep_for(100us);
        }
    }
    if (! completed) {
        return error_result(sql::error::Code::CODE_UNSPECIFIED, "response timed out");
    }
    if (res->error_.code() != tateyama::proto::diagnostics::Code::UNKNOWN) {
        // error reported by the framework, not by the sql service
        std::string msg{tateyama::proto::diagnostics::Code_Name(res->error_.code())};
        msg.append(": ");
        msg.append(res->error_.message());
        return error_result(sql::error::Code::CODE_UNSPECIFIED, msg);
    }
    return {};
}

call_result service_client::begin(begin_options const& options, api::transaction_handle& out) {
    auto payload = utils::encode_begin(
        options.kind_ == begin_options::kind::rtx,
        options.kind_ == begin_options::kind::ltx,
        options.write_preserves_,
        options.label_,
        options.modifies_definitions_
    );
    std::shared_ptr<test_response> res{};
    if (auto ret = send(payload, res); ! ret) {
        return ret;
    }
    sql::response::Response resp{};
    if (! parse_response(res->body_, resp) || ! resp.has_begin()) {
        return missing_response("begin");
    }
    auto& begin = resp.begin();
    if (! begin.has_success()) {
        return from_error(begin.error());
    }
    out = api::transaction_handle{begin.success().transaction_handle().handle()};
    return {};
}

call_result service_client::commit(api::transaction_handle tx) {
    std::shared_ptr<test_response> res{};
    if (auto ret = send(utils::encode_commit(tx, true), res); ! ret) {
        return ret;
    }
    return decode_result_only(res->body_);
}

call_result service_client::rollback(api::transaction_handle tx) {
    std::shared_ptr<test_response> res{};
    if (auto ret = send(utils::encode_rollback(tx), res); ! ret) {
        return ret;
    }
    return decode_result_only(res->body_);
}

call_result service_client::prepare(
    std::string_view sql,
    std::unordered_map<std::string, sql::common::AtomType> const& placeholders,
    std::uint64_t& out,
    bool* has_result_records
) {
    std::shared_ptr<test_response> res{};
    if (auto ret = send(utils::encode_prepare_vars(std::string{sql}, placeholders), res); ! ret) {
        return ret;
    }
    sql::response::Response resp{};
    if (! parse_response(res->body_, resp) || ! resp.has_prepare()) {
        return missing_response("prepare");
    }
    auto& prep = resp.prepare();
    if (! prep.has_prepared_statement_handle()) {
        return from_error(prep.error());
    }
    out = prep.prepared_statement_handle().handle();
    if (has_result_records != nullptr) {
        *has_result_records = prep.prepared_statement_handle().has_result_records();
    }
    return {};
}

call_result service_client::dispose_statement(std::uint64_t stmt) {
    std::shared_ptr<test_response> res{};
    if (auto ret = send(utils::encode_dispose_prepare(stmt), res); ! ret) {
        return ret;
    }
    return decode_result_only(res->body_);
}

call_result service_client::execute_statement(
    api::transaction_handle tx,
    std::string_view sql,
    std::shared_ptr<request_statistics>* stats
) {
    std::shared_ptr<test_response> res{};
    if (auto ret = send(utils::encode_execute_statement(tx, sql), res); ! ret) {
        return ret;
    }
    return decode_execute_result(res->body_, stats);
}

call_result service_client::execute_prepared_statement(
    api::transaction_handle tx,
    std::uint64_t stmt,
    std::vector<utils::parameter> const& parameters,
    std::shared_ptr<request_statistics>* stats
) {
    std::shared_ptr<test_response> res{};
    if (auto ret = send(utils::encode_execute_prepared_statement(tx, stmt, parameters), res); ! ret) {
        return ret;
    }
    return decode_execute_result(res->body_, stats);
}

call_result service_client::execute_query_common(
    std::string_view payload,
    data_handler on_data,
    query_info* info
) {
    std::shared_ptr<test_response> res{};
    if (auto ret = send(payload, res, std::move(on_data)); ! ret) {
        return ret;
    }
    if (auto ret = decode_result_only(res->body_); ! ret) {
        return ret;
    }
    if (info != nullptr) {
        sql::response::Response resp{};
        if (! parse_response(res->body_head_, resp) || ! resp.has_execute_query()) {
            return missing_response("execute_query");
        }
        auto& eq = resp.execute_query();
        info->name_ = eq.name();
        info->columns_ = utils::create_common_column(eq.record_meta());
    }
    return {};
}

call_result service_client::execute_query(
    api::transaction_handle tx,
    std::string_view sql,
    data_handler on_data,
    query_info* info
) {
    return execute_query_common(utils::encode_execute_query(tx, sql), std::move(on_data), info);
}

call_result service_client::execute_prepared_query(
    api::transaction_handle tx,
    std::uint64_t stmt,
    std::vector<utils::parameter> const& parameters,
    data_handler on_data,
    query_info* info
) {
    return execute_query_common(
        utils::encode_execute_prepared_query(tx, stmt, parameters),
        std::move(on_data),
        info
    );
}

namespace {

call_result decode_explain(std::string_view body, std::string& out) {
    sql::response::Response resp{};
    if (! parse_response(body, resp) || ! resp.has_explain()) {
        return missing_response("explain");
    }
    auto& explain = resp.explain();
    if (explain.has_error()) {
        return from_error(explain.error());
    }
    out = explain.success().contents();
    return {};
}

}  // namespace

call_result service_client::explain(
    std::uint64_t stmt,
    std::vector<utils::parameter> const& parameters,
    std::string& out
) {
    std::shared_ptr<test_response> res{};
    if (auto ret = send(utils::encode_explain(stmt, parameters), res); ! ret) {
        return ret;
    }
    return decode_explain(res->body_, out);
}

call_result service_client::explain(std::string_view sql, std::string& out) {
    std::shared_ptr<test_response> res{};
    if (auto ret = send(utils::encode_explain_by_text(sql), res); ! ret) {
        return ret;
    }
    return decode_explain(res->body_, out);
}

}  // namespace jogasaki::common_cli
