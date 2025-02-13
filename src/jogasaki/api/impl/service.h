/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <boost/assert.hpp>
#include <boost/stacktrace/stacktrace.hpp>
#include <tbb/concurrent_hash_map.h>

#include <takatori/type/data.h>
#include <takatori/type/date.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>
#include <takatori/util/reference_list_view.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/table.h>
#include <tateyama/api/configuration.h>
#include <tateyama/api/server/data_channel.h>
#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>
#include <tateyama/api/server/writer.h>
#include <tateyama/framework/service.h>
#include <tateyama/status.h>
#include <tateyama/utils/cache_align.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/error_info.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/impl/data_channel.h>
#include <jogasaki/api/impl/data_writer.h>
#include <jogasaki/api/impl/error_info.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/record_meta.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/io/dump_config.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/proto/sql/response.pb.h>
#include <jogasaki/request_info.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/sanitize_utf8.h>
#include <jogasaki/utils/string_manipulation.h>

#include "map_error_code.h"

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;

namespace sql = jogasaki::proto::sql;

namespace details {

class query_info;

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
T* mutable_object(sql::response::Response& r) {
    if constexpr (std::is_same_v<T, sql::response::Begin>) {  //NOLINT
        return r.mutable_begin();
    } else if constexpr (std::is_same_v<T, sql::response::Prepare>) {  //NOLINT
        return r.mutable_prepare();
    } else if constexpr (std::is_same_v<T, sql::response::ResultOnly>) {  //NOLINT
        return r.mutable_result_only();
    } else if constexpr (std::is_same_v<T, sql::response::ExecuteQuery>) {  //NOLINT
        return r.mutable_execute_query();
    } else if constexpr (std::is_same_v<T, sql::response::Explain>) {  //NOLINT
        return r.mutable_explain();
    } else if constexpr (std::is_same_v<T, sql::response::DescribeTable>) {  //NOLINT
        return r.mutable_describe_table();
    } else if constexpr (std::is_same_v<T, sql::response::ListTables>) {  //NOLINT
        return r.mutable_list_tables();
    } else if constexpr (std::is_same_v<T, sql::response::GetErrorInfo>) {  //NOLINT
        return r.mutable_get_error_info();
    } else if constexpr (std::is_same_v<T, sql::response::ExecuteResult>) {  //NOLINT
        return r.mutable_execute_result();
    } else if constexpr (std::is_same_v<T, sql::response::ExtractStatementInfo>) {  //NOLINT
        return r.mutable_extract_statement_info();
    } else {
        static_fail();
    }
}

void report_error(
    tateyama::api::server::response& res,
    tateyama::proto::diagnostics::Code code,
    std::string_view msg,
    std::size_t reqid
);

inline bool promote_error_if_needed(
    tateyama::api::server::response& res,
    error::error_info* err_info,
    request_info const& req_info
) {
    if(! err_info || err_info->code() != error_code::request_canceled) {
        return false;
    }
    report_error(res, tateyama::proto::diagnostics::Code::OPERATION_CANCELED, err_info->message(), req_info.id());
    return true;
}

template<typename T>
void error(
    tateyama::api::server::response& res,
    error::error_info* err_info,
    request_info const& req_info
) {
    if(promote_error_if_needed(res, err_info, req_info)) {
        return;
    }
    sql::response::Response r{};
    auto* p = mutable_object<T>(r);
    auto* e = p->mutable_error();
    if(! err_info) {
        // missing error info. this is programming error.
        // empty error code results in SQL_SERVICE_EXCEPTION in the client with no error message
        LOG_LP(ERROR) << "unexpected error occurred and missing error info. " << ::boost::stacktrace::stacktrace{};
    }
    e->set_code(map_error(err_info ? err_info->code() : error_code::none));
    std::string detail{utils::sanitize_utf8(err_info ? err_info->message() : "")};
    e->set_detail(detail);
    std::string suptext{utils::sanitize_utf8(err_info ? err_info->supplemental_text() : "")};
    e->set_supplemental_text(suptext);
    reply(res, r, req_info);
}

template<typename T>
void error(
    tateyama::api::server::response& res,
    api::error_info* err_info,
    request_info const& req_info
) {
    error::error_info* p{};
    if (! err_info) {
        return error<T>(res, p, req_info);
    }
    p = dynamic_cast<api::impl::error_info*>(err_info)->body().get();
    return error<T>(res, p, req_info);
}

template<typename T, typename... Args>
void success(tateyama::api::server::response& res, Args...) = delete; //NOLINT(performance-unnecessary-value-param)

template<>
inline void success<sql::response::ResultOnly>(
    tateyama::api::server::response& res,
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::response::Response r{};
    auto* ro = r.mutable_result_only();
    ro->mutable_success();
    reply(res, r, req_info);
}

template<>
inline void success<sql::response::Begin>(
    tateyama::api::server::response& res,
    jogasaki::api::transaction_handle tx, //NOLINT(performance-unnecessary-value-param)
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::response::Response r{};
    auto* b = r.mutable_begin();
    auto* s = b->mutable_success();
    auto* tid = s->mutable_transaction_id();
    auto* t = s->mutable_transaction_handle();
    auto idstr = tx.transaction_id();
    tid->set_id(idstr.data(), idstr.size());
    t->set_handle(static_cast<std::size_t>(tx));
    reply(res, r, req_info);
}

template<>
inline void success<sql::response::Prepare>(
    tateyama::api::server::response& res,
    jogasaki::api::statement_handle statement, //NOLINT(performance-unnecessary-value-param)
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::response::Response r{};
    auto* p = r.mutable_prepare();
    auto* ps = p->mutable_prepared_statement_handle();
    ps->set_handle(static_cast<std::size_t>(statement));
    ps->set_has_result_records(statement.has_result_records());
    reply(res, r, req_info);
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
        case k::time_of_day: {
            if(static_cast<takatori::type::time_of_day const&>(type).with_time_zone()) { //NOLINT
                return AtomType::TIME_OF_DAY_WITH_TIME_ZONE;
            }
            return AtomType::TIME_OF_DAY;
        }
        case k::time_point: {
            if(static_cast<takatori::type::time_point const&>(type).with_time_zone()) { //NOLINT
                return AtomType::TIME_POINT_WITH_TIME_ZONE;
            }
            return AtomType::TIME_POINT;
        }
        case k::blob: return AtomType::BLOB;
        case k::clob: return AtomType::CLOB;
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
    sql::response::Response r{};
    auto* explain = r.mutable_explain();
    auto* success = explain->mutable_success();
    success->set_format_version(sql_proto_explain_format_version);
    std::string id{sql_proto_explain_format_id};
    success->set_format_id(std::move(id));
    success->set_contents(std::move(output));
    set_metadata(meta, *success);
    reply(res, r, req_info);
}

template<>
inline void success<sql::response::DescribeTable>(
    tateyama::api::server::response& res,
    yugawara::storage::table const* tbl,
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    BOOST_ASSERT(tbl != nullptr); //NOLINT
    sql::response::Response r{};
    auto* dt = r.mutable_describe_table();
    auto* success = dt->mutable_success();
    success->set_table_name(std::string{tbl->simple_name()});
    success->set_schema_name("");  //FIXME schema resolution
    success->set_database_name("");  //FIXME database name resolution
    auto* cols = success->mutable_columns();
    for(auto&& col : tbl->columns()) {
        if(utils::is_prefix(col.simple_name(), generated_pkey_column_prefix)) {
            continue;
        }
        auto* c = cols->Add();
        c->set_name(std::string{col.simple_name()});
        c->set_atom_type(to_atom_type(col.type()));
    }
    reply(res, r, req_info);
}

template<>
inline void success<sql::response::ListTables>(
    tateyama::api::server::response& res,
    std::vector<std::string> simple_names,  //NOLINT(performance-unnecessary-value-param)
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::response::Response r{};
    auto* lt = r.mutable_list_tables();
    auto* success = lt->mutable_success();
    for(auto&& n : simple_names) {
        auto* name = success->add_table_path_names();
        name->add_identifiers()->set_label(n);
    }
    reply(res, r, req_info);
}

template<>
inline void success<sql::response::GetSearchPath>(
    tateyama::api::server::response& res,
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::response::Response r{};
    auto* sp = r.mutable_get_search_path();
    sp->mutable_success();

    // currently search path is not in place yet, so return empty success object

    reply(res, r, req_info);
}


template<>
inline void success<sql::response::GetErrorInfo>(
    tateyama::api::server::response& res,
    request_info req_info,  //NOLINT(performance-unnecessary-value-param)
    std::shared_ptr<api::error_info> info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::response::Response r{};
    auto* gei = r.mutable_get_error_info();
    if (! info) {
        gei->mutable_error_not_found();
    } else {
        auto* error = gei->mutable_success();
        error->set_code(map_error(info->code()));
        auto msg = info->message();
        error->set_detail(msg.data(), msg.size());
        auto text = info->supplemental_text();
        error->set_supplemental_text(text.data(), text.size());
    }
    reply(res, r, req_info);
}

inline sql::response::ExecuteResult::CounterType from(counter_kind kind) {
    using k = counter_kind;
    switch(kind) {
        case k::inserted: return sql::response::ExecuteResult::INSERTED_ROWS;
        case k::updated: return sql::response::ExecuteResult::UPDATED_ROWS;
        case k::merged: return sql::response::ExecuteResult::MERGED_ROWS;
        case k::deleted: return sql::response::ExecuteResult::DELETED_ROWS;
        default: return sql::response::ExecuteResult::COUNTER_TYPE_UNSPECIFIED;
    }
}

template<>
inline void success<sql::response::ExecuteResult>(
    tateyama::api::server::response& res,
    request_info req_info,  //NOLINT(performance-unnecessary-value-param)
    std::shared_ptr<request_statistics> stats  //NOLINT(performance-unnecessary-value-param)
) {
    sql::response::Response r{};
    auto* er = r.mutable_execute_result();
    auto* s = er->mutable_success();

    stats->each_counter([&](auto&& kind, auto&& counter){
        auto knd = from(kind);
        if(knd != sql::response::ExecuteResult::COUNTER_TYPE_UNSPECIFIED) {
            if(counter.count().has_value()) {
                auto* c = s->add_counters();
                c->set_type(knd);
                c->set_value(*counter.count());
            }
        }
    });
    reply(res, r, req_info);
}

template<>
inline void success<sql::response::ExtractStatementInfo>(
    tateyama::api::server::response& res,
    std::shared_ptr<std::string> sql_text,  //NOLINT(performance-unnecessary-value-param)
    std::string_view tx_id, //NOLINT(performance-unnecessary-value-param)
    request_info req_info  //NOLINT(performance-unnecessary-value-param)
) {
    sql::response::Response r{};
    auto* dt = r.mutable_extract_statement_info();
    auto* success = dt->mutable_success();
    success->set_sql(*sql_text);
    success->mutable_transaction_id()->set_id(std::string{tx_id});
    reply(res, r, req_info);
}

inline void send_body_head(
    tateyama::api::server::response& res,
    channel_info const& info,
    request_info const& req_info
) {
    sql::response::Response r{};
    auto* e = r.mutable_execute_query();
    e->set_name(info.name_);
    auto* meta = e->mutable_record_meta();
    set_metadata(info.meta_, *meta);
    details::reply(res, r, req_info, true);
}

}

class service {
public:
    service() = default;

    service(std::shared_ptr<tateyama::api::configuration::whole> cfg, jogasaki::api::database* db);

    bool operator()(
        std::shared_ptr<tateyama::api::server::request> req,
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

        cache_align static inline std::atomic_size_t id_src_{0};  //NOLINT
    };

    std::shared_ptr<tateyama::api::configuration::whole> cfg_{};
    jogasaki::api::database* db_{};
    tbb::concurrent_hash_map<std::size_t, std::shared_ptr<callback_control>> callbacks_{};
    cache_align static inline std::atomic_size_t request_id_src_{0};  //NOLINT

    bool process(
            std::shared_ptr<tateyama::api::server::request> req,
            std::shared_ptr<tateyama::api::server::response> res
    );

    void command_begin(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );

    void command_prepare(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );
    void command_execute_statement(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );

    void command_execute_query(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );
    void command_execute_prepared_statement(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );
    void command_execute_prepared_query(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );
    void command_execute_dump(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );
    void command_execute_load(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );

    void command_commit(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );

    void command_rollback(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );
    void command_dispose_prepared_statement(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );
    void command_explain(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );
    void command_explain_by_text(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );

    void command_describe_table(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );
    void command_list_tables(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );
    void command_get_search_path(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );
    void command_get_error_info(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );
    void command_dispose_transaction(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );
    void command_extract_statement_info(
        sql::request::Request const& proto_req,
        std::shared_ptr<tateyama::api::server::response> const& res,
        request_info const& req_info
    );

    void execute_statement(
        std::shared_ptr<tateyama::api::server::response> const& res,
        std::shared_ptr<jogasaki::api::executable_statement> stmt,
        jogasaki::api::transaction_handle tx,
        request_info const& req_info
    );
    void execute_query(
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::query_info const& q,
        jogasaki::api::transaction_handle tx,
        request_info const& req_info
    );

    void execute_dump(
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::query_info const& q,
        jogasaki::api::transaction_handle tx,
        std::string_view directory,
        executor::io::dump_config const& opts,
        request_info const& req_info
    );
    void execute_load(
        std::shared_ptr<tateyama::api::server::response> const& res,
        details::query_info const& q,
        jogasaki::api::transaction_handle tx,
        std::vector<std::string> const& files,
        request_info const& req_info
    );
    void set_params(
        ::google::protobuf::RepeatedPtrField<sql::request::Parameter> const& ps,
        std::unique_ptr<jogasaki::api::parameter_set>& params,
        request_info const& req_info
    );
    [[nodiscard]] std::size_t new_resultset_id() const noexcept;

    /**
     * @brief caluculate the count of the write_count
     * @param es the executable_statement
     * @return count of the write_count
     */
    [[nodiscard]] std::size_t get_write_count(
        jogasaki::api::executable_statement const& es) const noexcept;
};

// public for testing purpose
bool extract_sql_and_tx_id(
    sql::request::Request const& req,
    api::database* db,
    std::shared_ptr<std::string>& sql_text,
    std::string& tx_id,
    std::shared_ptr<error::error_info>& err_info
);

}  // namespace jogasaki::api::impl
