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

#include <string_view>
#include <atomic>
#include <memory>

#include <takatori/util/downcast.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/statement_handle.h>

#include <tateyama/status.h>
#include <tateyama/api/endpoint/service.h>
#include <tateyama/api/endpoint/response.h>
#include <tateyama/api/endpoint/writer.h>
#include <tateyama/api/endpoint/data_channel.h>

#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"
#include "common.pb.h"

namespace tateyama::api::endpoint::impl {

using takatori::util::unsafe_downcast;

class output {
public:
    std::unique_ptr<jogasaki::api::result_set> result_set_{};  //NOLINT
    std::unique_ptr<jogasaki::api::result_set_iterator> iterator_{};  //NOLINT
    std::unique_ptr<jogasaki::api::prepared_statement> prepared_{};  //NOLINT
    std::string wire_name_;  //NOLINT
    tateyama::api::endpoint::writer* writer_{};  //NOLINT
    tateyama::api::endpoint::data_channel* data_channel_{};  //NOLINT
};

class service : public api::endpoint::service {
public:
    service() = default;

    explicit service(jogasaki::api::database& db) :
        db_(std::addressof(db))
    {}

    tateyama::status operator()(
        std::shared_ptr<tateyama::api::endpoint::request const> req,
        std::shared_ptr<tateyama::api::endpoint::response> res
    ) override;

private:
    jogasaki::api::database* db_{};
    std::atomic_size_t resultset_id_{};

    [[nodiscard]] const char* execute_statement(std::string_view, jogasaki::api::transaction_handle tx);
    [[nodiscard]] const char* execute_prepared_statement(
        std::size_t,
        jogasaki::api::parameter_set&,
        jogasaki::api::transaction_handle tx
    );
    void process_output(output& out);
    [[nodiscard]] const char* execute_query(
        tateyama::api::endpoint::response& res,
        std::string_view,
        std::size_t,
        jogasaki::api::transaction_handle tx,
        std::unique_ptr<output>& out
    );
    [[nodiscard]] const char* execute_prepared_query(
        tateyama::api::endpoint::response& res,
        std::size_t,
        jogasaki::api::parameter_set&,
        std::size_t,
        jogasaki::api::transaction_handle tx,
        std::unique_ptr<output>& out
    );
    void set_metadata(output const& out, schema::RecordMeta&);
    void set_params(::request::ParameterSet const&, std::unique_ptr<jogasaki::api::parameter_set>&);
    void release_writers(tateyama::api::endpoint::response& res, output& out);
    void reply(endpoint::response& res, ::response::Response &r);

    template<typename T>
    void error(endpoint::response&, std::string) = delete; //NOLINT(performance-unnecessary-value-param)
    template<typename T, typename... Args>
    void success(endpoint::response& res, Args...args) = delete; //NOLINT(performance-unnecessary-value-param)
};

inline void set_application_error(endpoint::response& res) {
    res.code(response_code::application_error);
    res.message("error on application domain - check response body");
}

template<>
inline void service::error<::response::Begin>(endpoint::response& res, std::string msg) {  //NOLINT(performance-unnecessary-value-param)
    ::response::Error e{};
    ::response::Begin p{};
    ::response::Response r{};

    e.set_detail(msg);
    p.set_allocated_error(&e);
    r.set_allocated_begin(&p);
    reply(res, r);
    r.release_begin();
    p.release_error();
    set_application_error(res);
}

template<>
inline void service::error<::response::Prepare>(endpoint::response& res, std::string msg) {  //NOLINT(performance-unnecessary-value-param)
    ::response::Error e{};
    ::response::Prepare p{};
    ::response::Response r{};

    e.set_detail(msg);
    p.set_allocated_error(&e);
    r.set_allocated_prepare(&p);
    reply(res, r);
    r.release_prepare();
    p.release_error();
    set_application_error(res);
}

template<>
inline void service::error<::response::ResultOnly>(endpoint::response& res, std::string msg) {  //NOLINT(performance-unnecessary-value-param)
    ::response::Error e{};
    ::response::ResultOnly ro{};
    ::response::Response r{};

    e.set_detail(msg);
    ro.set_allocated_error(&e);
    r.set_allocated_result_only(&ro);
    reply(res, r);
    r.release_result_only();
    ro.release_error();
    set_application_error(res);
}

template<>
inline void service::error<::response::ExecuteQuery>(endpoint::response& res, std::string msg) {  //NOLINT(performance-unnecessary-value-param)
    ::response::Error e{};
    ::response::ExecuteQuery eq{};
    ::response::Response r{};

    e.set_detail(msg);
    eq.set_allocated_error(&e);
    r.set_allocated_execute_query(&eq);
    reply(res, r);
    r.release_execute_query();
    eq.release_error();
    set_application_error(res);
}

template<>
inline void service::success<::response::ResultOnly>(endpoint::response& res) {  //NOLINT(performance-unnecessary-value-param)
    ::response::Success s{};
    ::response::ResultOnly ro{};
    ::response::Response r{};

    ro.set_allocated_success(&s);
    r.set_allocated_result_only(&ro);
    reply(res, r);
    r.release_result_only();
    ro.release_success();
}

template<>
inline void service::success<::response::Begin>(endpoint::response& res, jogasaki::api::transaction_handle tx) {  //NOLINT(performance-unnecessary-value-param)
    ::common::Transaction t{};
    ::response::Begin b{};
    ::response::Response r{};

    t.set_handle(static_cast<std::size_t>(tx));
    b.set_allocated_transaction_handle(&t);
    r.set_allocated_begin(&b);
    reply(res, r);
    r.release_begin();
    b.release_transaction_handle();
}

template<>
inline void service::success<::response::Prepare>(endpoint::response& res, jogasaki::api::statement_handle statement) {  //NOLINT(performance-unnecessary-value-param)
    ::common::PreparedStatement ps{};
    ::response::Prepare p{};
    ::response::Response r{};

    ps.set_handle(static_cast<std::size_t>(statement));
    p.set_allocated_prepared_statement_handle(&ps);
    r.set_allocated_prepare(&p);
    reply(res, r);
    r.release_prepare();
    p.release_prepared_statement_handle();
}

template<>
inline void service::success<::response::ExecuteQuery>(endpoint::response& res, output* out) {  //NOLINT(performance-unnecessary-value-param)
    ::schema::RecordMeta meta{};
    ::response::ResultSetInfo i{};
    ::response::ExecuteQuery e{};
    ::response::Response r{};

    set_metadata(*out, meta);
    i.set_name(out->wire_name_);
    i.set_allocated_record_meta(&meta);
    e.set_allocated_result_set_info(&i);
    r.set_allocated_execute_query(&e);
    reply(res, r);
    r.release_execute_query();
    e.release_result_set_info();
    i.release_record_meta();
}

inline api::endpoint::impl::service& get_impl(api::endpoint::service& svc) {
    return unsafe_downcast<api::endpoint::impl::service>(svc);
}

}

