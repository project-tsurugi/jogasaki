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

#include <tbb/concurrent_hash_map.h>

#include <takatori/util/downcast.h>
#include <takatori/util/fail.h>

#include <jogasaki/constants.h>
#include <jogasaki/api/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/impl/data_channel.h>
#include <jogasaki/api/impl/data_writer.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/string_manipulation.h>
#include <jogasaki/utils/sanitize_utf8.h>

#include <tateyama/status.h>

#include <tateyama/framework/service.h>
#include <tateyama/api/configuration.h>
#include <tateyama/api/server/response.h>
#include <tateyama/api/server/writer.h>
#include <tateyama/api/server/data_channel.h>
#include <tateyama/api/server/response_code.h>

#include "jogasaki/proto/sql/request.pb.h"
#include "jogasaki/proto/sql/response.pb.h"
#include "jogasaki/proto/sql/common.pb.h"
#include "jogasaki/proto/sql/status.pb.h"

#include "map_error_code.h"

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;
using takatori::util::fail;

using response_code = tateyama::api::server::response_code;

namespace sql = jogasaki::proto::sql;

namespace details {

class query_info;

class request_info {
public:
    request_info() = default;

    explicit request_info(std::size_t id) :
        id_(id)
    {}

    [[nodiscard]] std::size_t id() const noexcept {
        return id_;
    }

private:
    std::size_t id_{};
};

struct cache_align channel_info {
    jogasaki::api::record_meta const* meta_{};  //NOLINT
    std::string name_;  //NOLINT
    std::shared_ptr<jogasaki::api::impl::data_channel> data_channel_{};  //NOLINT
};

void reply(
    tateyama::api::server::response& res,
    sql::response::Response& r,
    request_info const& req_info,
    bool body_head = false
);

template <class T>
void set_metadata(jogasaki::api::record_meta const* metadata, T& meta);

template<bool flag = false> void static_fail() {
    static_assert(flag);
}

template<typename T>
void set_allocated_object(sql::response::Response& r, T& p) {
    if constexpr (std::is_same_v<T, sql::response::Begin>) {  //NOLINT
        r.set_allocated_begin(&p);
    } else if constexpr (std::is_same_v<T, sql::response::Prepare>) {  //NOLINT
        r.set_allocated_prepare(&p);
    } else if constexpr (std::is_same_v<T, sql::response::ResultOnly>) {  //NOLINT
        r.set_allocated_result_only(&p);
    } else if constexpr (std::is_same_v<T, sql::response::ExecuteQuery>) {  //NOLINT
        r.set_allocated_execute_query(&p);
    } else if constexpr (std::is_same_v<T, sql::response::Explain>) {  //NOLINT
        r.set_allocated_explain(&p);
    } else if constexpr (std::is_same_v<T, sql::response::DescribeTable>) {  //NOLINT
        r.set_allocated_describe_table(&p);
    } else if constexpr (std::is_same_v<T, sql::response::ListTables>) {  //NOLINT
        r.set_allocated_list_tables(&p);
    } else if constexpr (std::is_same_v<T, sql::response::GetErrorInfo>) {  //NOLINT
        r.set_allocated_get_error_info(&p);
    } else {
        static_fail();
    }
}

template<typename T>
void release_object(sql::response::Response& r, T&) {
    if constexpr (std::is_same_v<T, sql::response::Begin>) {  //NOLINT
        r.release_begin();
    } else if constexpr (std::is_same_v<T, sql::response::Prepare>) {  //NOLINT
        r.release_prepare();
    } else if constexpr (std::is_same_v<T, sql::response::ResultOnly>) {  //NOLINT
        r.release_result_only();
    } else if constexpr (std::is_same_v<T, sql::response::ExecuteQuery>) {  //NOLINT
        r.release_execute_query();
    } else if constexpr (std::is_same_v<T, sql::response::Explain>) {  //NOLINT
        r.release_explain();
    } else if constexpr (std::is_same_v<T, sql::response::DescribeTable>) {  //NOLINT
        r.release_describe_table();
    } else if constexpr (std::is_same_v<T, sql::response::ListTables>) {  //NOLINT
        r.release_list_tables();
    } else if constexpr (std::is_same_v<T, sql::response::GetErrorInfo>) {  //NOLINT
        r.release_get_error_info();
    } else {
        static_fail();
    }
}

inline sql::status::Status map_status(jogasaki::status s) {
    switch(s) {
        case jogasaki::status::ok: return sql::status::Status::OK;
        case jogasaki::status::not_found: return sql::status::Status::NOT_FOUND;
        case jogasaki::status::already_exists: return sql::status::Status::ALREADY_EXISTS;
        case jogasaki::status::user_rollback: return sql::status::Status::USER_ROLLBACK;
        case jogasaki::status::err_unknown: return sql::status::Status::ERR_UNKNOWN;
        case jogasaki::status::err_io_error: return sql::status::Status::ERR_IO_ERROR;
        case jogasaki::status::err_parse_error: return sql::status::Status::ERR_PARSE_ERROR;
        case jogasaki::status::err_translator_error: return sql::status::Status::ERR_TRANSLATOR_ERROR;
        case jogasaki::status::err_compiler_error: return sql::status::Status::ERR_COMPILER_ERROR;
        case jogasaki::status::err_invalid_argument: return sql::status::Status::ERR_INVALID_ARGUMENT;
        case jogasaki::status::err_invalid_state: return sql::status::Status::ERR_INVALID_STATE;
        case jogasaki::status::err_unsupported: return sql::status::Status::ERR_UNSUPPORTED;
        case jogasaki::status::err_user_error: return sql::status::Status::ERR_USER_ERROR;
        case jogasaki::status::err_aborted: return sql::status::Status::ERR_ABORTED;
        case jogasaki::status::err_serialization_failure: return sql::status::Status::ERR_SERIALIZATION_FAILURE;
        case jogasaki::status::err_not_found: return sql::status::Status::ERR_NOT_FOUND;
        case jogasaki::status::err_already_exists: return sql::status::Status::ERR_ALREADY_EXISTS;
        case jogasaki::status::err_inconsistent_index: return sql::status::Status::ERR_INCONSISTENT_INDEX;
        case jogasaki::status::err_time_out: return sql::status::Status::ERR_TIME_OUT;
        case jogasaki::status::err_integrity_constraint_violation: return sql::status::Status::ERR_INTEGRITY_CONSTRAINT_VIOLATION;
        case jogasaki::status::err_expression_evaluation_failure: return sql::status::Status::ERR_EXPRESSION_EVALUATION_FAILURE;
        case jogasaki::status::err_unresolved_host_variable: return sql::status::Status::ERR_UNRESOLVED_HOST_VARIABLE;
        case jogasaki::status::err_type_mismatch: return sql::status::Status::ERR_TYPE_MISMATCH;
        case jogasaki::status::err_not_implemented: return sql::status::Status::ERR_NOT_IMPLEMENTED;
        case jogasaki::status::err_illegal_operation: return sql::status::Status::ERR_ILLEGAL_OPERATION;
        case jogasaki::status::err_missing_operation_target: return sql::status::Status::ERR_MISSING_OPERATION_TARGET;
        case jogasaki::status::err_conflict_on_write_preserve: return sql::status::Status::ERR_CONFLICT_ON_WRITE_PRESERVE;
        case jogasaki::status::err_inactive_transaction: return sql::status::Status::ERR_INACTIVE_TRANSACTION;
        case jogasaki::status::err_data_corruption: return sql::status::Status::ERR_DATA_CORRUPTION;
        case jogasaki::status::err_resource_limit_reached: return sql::status::Status::ERR_RESOURCE_LIMIT_REACHED;
        case jogasaki::status::err_unique_constraint_violation: return sql::status::Status::ERR_UNIQUE_CONSTRAINT_VIOLATION;
        case jogasaki::status::waiting_for_other_transaction: return sql::status::Status::ERR_UNKNOWN;  // wait_for_transaction is internal, should not be exposed
    }
    fail();
}

template<typename T>
void error(
    tateyama::api::server::response& res,
    jogasaki::status s,
    std::string_view msg,
    request_info const& req_info
) { 
    sql::response::Error e{};
    T p{};
    sql::response::Response r{};
    e.set_status(map_status(s));
    std::string m{utils::sanitize_utf8(msg)};
    e.set_detail(m);
    p.set_allocated_error(&e);
    set_allocated_object(r, p);
    res.code(response_code::application_error);
    reply(res, r, req_info);
    release_object(r, p);
    p.release_error();
}

template<typename T>
void error(
    tateyama::api::server::response& res,
    api::error_info* err_info,
    request_info const& req_info
) {
    sql::response::Error e{};
    T p{};
    sql::response::Response r{};
    e.set_status(map_status(err_info ? err_info->status() : status::ok));
    e.set_code(map_error(err_info ? err_info->code() : error_code::none));
    std::string detail{utils::sanitize_utf8(err_info ? err_info->message() : "")};
    e.set_detail(detail);
    std::string suptext{utils::sanitize_utf8(err_info ? err_info->supplemental_text() : "")};
    e.set_supplemental_text(suptext);
    p.set_allocated_error(&e);
    set_allocated_object(r, p);
    res.code(response_code::application_error);
    reply(res, r, req_info);
    release_object(r, p);
    p.release_error();
}

template<typename T, typename... Args>
void success(tateyama::api::server::response& res, Args...) = delete; //NOLINT(performance-unnecessary-value-param)

template<>
inline void success<sql::response::ResultOnly>(
    tateyama::api::server::response& res,
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::response::Success s{};
    sql::response::ResultOnly ro{};
    sql::response::Response r{};

    ro.set_allocated_success(&s);
    r.set_allocated_result_only(&ro);
    res.code(response_code::success);
    reply(res, r, req_info);
    r.release_result_only();
    ro.release_success();
}

template<>
inline void success<sql::response::Begin>(
    tateyama::api::server::response& res,
    jogasaki::api::transaction_handle tx, //NOLINT(performance-unnecessary-value-param)
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::common::Transaction t{};
    sql::common::TransactionId tid{};
    sql::response::Begin b{};
    sql::response::Begin::Success s{};
    sql::response::Response r{};

    auto idstr = tx.transaction_id();
    tid.set_id(idstr.data(), idstr.size());
    t.set_handle(static_cast<std::size_t>(tx));
    s.set_allocated_transaction_handle(&t);
    s.set_allocated_transaction_id(&tid);
    b.set_allocated_success(&s);
    r.set_allocated_begin(&b);
    res.code(response_code::success);
    reply(res, r, req_info);
    r.release_begin();
    b.release_success();
    s.release_transaction_id();
    s.release_transaction_handle();
}

template<>
inline void success<sql::response::Prepare>(
    tateyama::api::server::response& res,
    jogasaki::api::statement_handle statement, //NOLINT(performance-unnecessary-value-param)
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::common::PreparedStatement ps{};
    sql::response::Prepare p{};
    sql::response::Response r{};

    ps.set_handle(static_cast<std::size_t>(statement));
    ps.set_has_result_records(statement.has_result_records());
    p.set_allocated_prepared_statement_handle(&ps);
    r.set_allocated_prepare(&p);
    res.code(response_code::success);
    reply(res, r, req_info);
    r.release_prepare();
    p.release_prepared_statement_handle();
}

inline ::jogasaki::proto::sql::common::AtomType to_atom_type(takatori::type::data const& type) {
    using k = takatori::type::type_kind;
    using AtomType = ::jogasaki::proto::sql::common::AtomType;
    switch(type.kind()) {
        case k::boolean: return AtomType::BOOLEAN;
        case k::int4: return AtomType::INT4;
        case k::int8: return AtomType::INT8;
        case k::float4: return AtomType::FLOAT4;
        case k::float8: return AtomType::FLOAT8;
        case k::decimal: return AtomType::DECIMAL;
        case k::character: return AtomType::CHARACTER;
        case k::octet: return AtomType::OCTET;
        case k::bit: return AtomType::BIT;
        case k::date: return AtomType::DATE;
        case k::time_of_day: return AtomType::TIME_OF_DAY;
        case k::time_point: return AtomType::TIME_POINT;
        case k::datetime_interval: return AtomType::DATETIME_INTERVAL;
        default:
            return AtomType::UNKNOWN;
    }
}

template<>
inline void success<sql::response::Explain>(
    tateyama::api::server::response& res,
    std::string output, //NOLINT(performance-unnecessary-value-param)
    api::record_meta const* meta,
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::response::Explain explain{};
    sql::response::Response r{};
    sql::response::Explain::Success success{};
    explain.set_allocated_success(&success);
    success.set_format_version(sql_proto_explain_format_version);
    std::string id{sql_proto_explain_format_id};
    success.set_allocated_format_id(&id);
    success.set_allocated_contents(&output);
    r.set_allocated_explain(&explain);
    set_metadata(meta, success);
    res.code(response_code::success);
    reply(res, r, req_info);
    success.clear_columns();
    r.release_explain();
    success.release_format_id();
    success.release_contents();
    explain.release_success();
}

template<>
inline void success<sql::response::DescribeTable>(
    tateyama::api::server::response& res,
    yugawara::storage::table const* tbl,
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    BOOST_ASSERT(tbl != nullptr); //NOLINT
    sql::response::Response r{};
    sql::response::DescribeTable dt{};
    r.set_allocated_describe_table(&dt);
    sql::response::DescribeTable_Success success{};
    dt.set_allocated_success(&success);
    success.set_table_name(std::string{tbl->simple_name()});
    success.set_schema_name("");  //FIXME schema resolution
    success.set_database_name("");  //FIXME database name resolution
    auto* cols = success.mutable_columns();
    for(auto&& col : tbl->columns()) {
        if(utils::is_prefix(col.simple_name(), generated_pkey_column_prefix)) {
            continue;
        }
        auto* c = cols->Add();
        c->set_name(std::string{col.simple_name()});
        c->set_atom_type(to_atom_type(col.type()));
    }
    res.code(response_code::success);
    reply(res, r, req_info);
    success.clear_columns();
    dt.release_success();
    r.release_describe_table();
}

template<>
inline void success<sql::response::ListTables>(
    tateyama::api::server::response& res,
    std::vector<std::string> simple_names,  //NOLINT(performance-unnecessary-value-param)
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::response::Response r{};
    sql::response::ListTables lt{};
    r.set_allocated_list_tables(&lt);
    sql::response::ListTables_Success success{};
    lt.set_allocated_success(&success);
    for(auto&& n : simple_names) {
        auto* name = success.add_table_path_names();
        name->add_identifiers()->set_label(n);
    }
    res.code(response_code::success);
    reply(res, r, req_info);
    lt.release_success();
    r.release_list_tables();
}

template<>
inline void success<sql::response::GetSearchPath>(
    tateyama::api::server::response& res,
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::response::Response r{};
    sql::response::GetSearchPath sp{};
    r.set_allocated_get_search_path(&sp);
    sql::response::GetSearchPath_Success success{};
    sp.set_allocated_success(&success);

    // currently search path is not in place yet, so return empty success object

    res.code(response_code::success);
    reply(res, r, req_info);
    sp.release_success();
    r.release_get_search_path();
}


template<>
inline void success<sql::response::GetErrorInfo>(
    tateyama::api::server::response& res,
    request_info req_info,  //NOLINT(performance-unnecessary-value-param)
    std::shared_ptr<api::error_info> info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::response::Response r{};
    sql::response::GetErrorInfo gei{};
    r.set_allocated_get_error_info(&gei);

    sql::response::Void v{};
    sql::response::Error error{};
    if (! info) {
        gei.set_allocated_error_not_found(&v);
    } else {
        gei.set_allocated_success(&error);
        error.set_status(map_status(info->status()));
        error.set_code(map_error(info->code()));
        auto msg = info->message();
        error.set_detail(msg.data(), msg.size());
        auto text = info->supplemental_text();
        error.set_supplemental_text(text.data(), text.size());
    }
    res.code(response_code::success);
    reply(res, r, req_info);
    if (! info) {
        gei.release_error_not_found();
    } else {
        gei.release_success();
    }
    r.release_get_error_info();
}

inline void send_body_head(
    tateyama::api::server::response& res,
    channel_info const& info,
    request_info const& req_info
) {
    sql::response::ResultSetMetadata meta{};
    sql::response::ExecuteQuery e{};
    sql::response::Response r{};

    set_metadata(info.meta_, meta);
    e.set_name(info.name_);
    e.set_allocated_record_meta(&meta);
    r.set_allocated_execute_query(&e);
    details::reply(res, r, req_info, true);
    r.release_execute_query();
    e.release_record_meta();
}

}

class service {
public:
    service() = default;

    service(std::shared_ptr<tateyama::api::configuration::whole> cfg, jogasaki::api::database* db);

    bool operator()(
        std::shared_ptr<tateyama::api::server::request const> req,
        std::shared_ptr<tateyama::api::server::response> res
    );

    bool start();

    bool shutdown(bool force = false);

    [[nodiscard]] jogasaki::api::database* database() const noexcept;
private:

    struct cache_align callback_control {
        explicit callback_control(std::shared_ptr<tateyama::api::server::response> response) :
            id_(id_src_++),
            response_(std::move(response))
        {}

        std::size_t id_{};  //NOLINT
        std::shared_ptr<tateyama::api::server::response> response_{};  //NOLINT
        std::unique_ptr<details::channel_info> channel_info_{};  //NOLINT

        static inline std::atomic_size_t id_src_{0};  //NOLINT
    };

    std::shared_ptr<tateyama::api::configuration::whole> cfg_{};
    jogasaki::api::database* db_{};
    tbb::concurrent_hash_map<std::size_t, std::shared_ptr<callback_control>> callbacks_{};
    static inline std::atomic_size_t request_id_src_{0};  //NOLINT

    bool process(
            std::shared_ptr<tateyama::api::server::request const> req,
            std::shared_ptr<tateyama::api::server::response> res
    );

    void command_begin(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );

    void command_prepare(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );
    void command_execute_statement(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );

    void command_execute_query(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );
    void command_execute_prepared_statement(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );
    void command_execute_prepared_query(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );
    void command_execute_dump(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );
    void command_execute_load(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );

    void command_commit(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );

    void command_rollback(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );
    void command_dispose_prepared_statement(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );
    void command_explain(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );

    void command_describe_table(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );
    void command_list_tables(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );
    void command_get_search_path(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );
    void command_get_error_info(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );
    void command_dispose_transaction(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::request_info const& req_info
    );

    void execute_statement(
        std::shared_ptr<tateyama::api::server::response> const& res,
        std::shared_ptr<jogasaki::api::executable_statement> stmt,
        jogasaki::api::transaction_handle tx,
        details::request_info const& req_info
    );
    void execute_query(
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::query_info const& q,
        jogasaki::api::transaction_handle tx,
        details::request_info const& req_info
    );

    struct dump_option {
        std::size_t max_records_per_file_{};
        bool keep_files_on_error_{};
    };

    void execute_dump(
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::query_info const& q,
        jogasaki::api::transaction_handle tx,
        std::string_view directory,
        dump_option const& opts,
        details::request_info const& req_info
    );
    void execute_load(
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::query_info const& q,
        jogasaki::api::transaction_handle tx,
        std::vector<std::string> const& files,
        details::request_info const& req_info
    );
    void set_params(::google::protobuf::RepeatedPtrField<sql::request::Parameter> const& ps, std::unique_ptr<jogasaki::api::parameter_set>& params);
    [[nodiscard]] std::size_t new_resultset_id() const noexcept;
};

}

