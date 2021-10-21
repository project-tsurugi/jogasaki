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

#include <jogasaki/api/database.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/impl/data_channel.h>
#include <jogasaki/api/impl/data_writer.h>

#include <tateyama/status.h>
#include <tateyama/api/environment.h>
#include <tateyama/api/server/service.h>
#include <tateyama/api/server/response.h>
#include <tateyama/api/server/writer.h>
#include <tateyama/api/server/data_channel.h>
#include <tateyama/api/endpoint/response_code.h>

#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"
#include "status.pb.h"

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;
using takatori::util::fail;

using response_code = tateyama::api::endpoint::response_code;

namespace details {

class query_info;

struct channel_info {
    jogasaki::api::record_meta const* meta_{};  //NOLINT
    std::string name_;  //NOLINT
    std::shared_ptr<jogasaki::api::impl::data_channel> data_channel_{};  //NOLINT
};

void reply(tateyama::api::server::response& res, ::response::Response& r, bool body_head = false);
void set_metadata(channel_info const& info, ::schema::RecordMeta& meta);

template<typename T>
void set_allocated_object(::response::Response& r, T& p) {
    if constexpr (std::is_same_v<T, ::response::Begin>) {  //NOLINT
        r.set_allocated_begin(&p);
    } else if constexpr (std::is_same_v<T, ::response::Prepare>) {  //NOLINT
        r.set_allocated_prepare(&p);
    } else if constexpr (std::is_same_v<T, ::response::ResultOnly>) {  //NOLINT
        r.set_allocated_result_only(&p);
    } else if constexpr (std::is_same_v<T, ::response::ExecuteQuery>) {  //NOLINT
        r.set_allocated_execute_query(&p);
    } else {
        fail();
    }
}

template<typename T>
void release_object(::response::Response& r, T&) {
    if constexpr (std::is_same_v<T, ::response::Begin>) {  //NOLINT
        r.release_begin();
    } else if constexpr (std::is_same_v<T, ::response::Prepare>) {  //NOLINT
        r.release_prepare();
    } else if constexpr (std::is_same_v<T, ::response::ResultOnly>) {  //NOLINT
        r.release_result_only();
    } else if constexpr (std::is_same_v<T, ::response::ExecuteQuery>) {  //NOLINT
        r.release_execute_query();
    } else {
        fail();
    }
}

inline ::status::Status map_status(jogasaki::status s) {
    switch(s) {
        case jogasaki::status::ok: return ::status::Status::OK;
        case jogasaki::status::not_found: return ::status::Status::NOT_FOUND;
        case jogasaki::status::already_exists: return ::status::Status::ALREADY_EXISTS;
        case jogasaki::status::user_rollback: return ::status::Status::USER_ROLLBACK;
        case jogasaki::status::err_unknown: return ::status::Status::ERR_UNKNOWN;
        case jogasaki::status::err_io_error: return ::status::Status::ERR_IO_ERROR;
        case jogasaki::status::err_parse_error: return ::status::Status::ERR_PARSE_ERROR;
        case jogasaki::status::err_translator_error: return ::status::Status::ERR_TRANSLATOR_ERROR;
        case jogasaki::status::err_compiler_error: return ::status::Status::ERR_COMPILER_ERROR;
        case jogasaki::status::err_invalid_argument: return ::status::Status::ERR_INVALID_ARGUMENT;
        case jogasaki::status::err_invalid_state: return ::status::Status::ERR_INVALID_STATE;
        case jogasaki::status::err_unsupported: return ::status::Status::ERR_UNSUPPORTED;
        case jogasaki::status::err_user_error: return ::status::Status::ERR_USER_ERROR;
        case jogasaki::status::err_aborted: return ::status::Status::ERR_ABORTED;
        case jogasaki::status::err_aborted_retryable: return ::status::Status::ERR_ABORTED_RETRYABLE;
        case jogasaki::status::err_not_found: return ::status::Status::ERR_NOT_FOUND;
        case jogasaki::status::err_already_exists: return ::status::Status::ERR_ALREADY_EXISTS;
        case jogasaki::status::err_inconsistent_index: return ::status::Status::ERR_INCONSISTENT_INDEX;
        case jogasaki::status::err_time_out: return ::status::Status::ERR_TIME_OUT;
    }
    fail();
}

template<typename T>
void error(tateyama::api::server::response& res, jogasaki::status s, std::string msg) { //NOLINT(performance-unnecessary-value-param)
    ::response::Error e{};
    T p{};
    ::response::Response r{};
    e.set_status(map_status(s));
    e.set_detail(msg);
    p.set_allocated_error(&e);
    set_allocated_object(r, p);
    res.code(response_code::application_error);
    reply(res, r);
    release_object(r, p);
    p.release_error();
}

template<typename T, typename... Args>
void success(tateyama::api::server::response& res, Args...) = delete; //NOLINT(performance-unnecessary-value-param)

template<>
inline void success<::response::ResultOnly>(tateyama::api::server::response& res) {  //NOLINT(performance-unnecessary-value-param)
    ::response::Success s{};
    ::response::ResultOnly ro{};
    ::response::Response r{};

    ro.set_allocated_success(&s);
    r.set_allocated_result_only(&ro);
    res.code(response_code::success);
    reply(res, r);
    r.release_result_only();
    ro.release_success();
}

template<>
inline void success<::response::Begin>(tateyama::api::server::response& res, jogasaki::api::transaction_handle tx) {  //NOLINT(performance-unnecessary-value-param)
    ::common::Transaction t{};
    ::response::Begin b{};
    ::response::Response r{};

    t.set_handle(static_cast<std::size_t>(tx));
    b.set_allocated_transaction_handle(&t);
    r.set_allocated_begin(&b);
    res.code(response_code::success);
    reply(res, r);
    r.release_begin();
    b.release_transaction_handle();
}

template<>
inline void success<::response::Prepare>(tateyama::api::server::response& res, jogasaki::api::statement_handle statement) {  //NOLINT(performance-unnecessary-value-param)
    ::common::PreparedStatement ps{};
    ::response::Prepare p{};
    ::response::Response r{};

    ps.set_handle(static_cast<std::size_t>(statement));
    p.set_allocated_prepared_statement_handle(&ps);
    r.set_allocated_prepare(&p);
    res.code(response_code::success);
    reply(res, r);
    r.release_prepare();
    p.release_prepared_statement_handle();
}

inline void send_body_head(tateyama::api::server::response& res, channel_info const& info) {  //NOLINT(performance-unnecessary-value-param)
    ::schema::RecordMeta meta{};
    ::response::ExecuteQuery e{};
    ::response::Response r{};

    set_metadata(info, meta);
    e.set_name(info.name_);
    e.set_allocated_record_meta(&meta);
    r.set_allocated_execute_query(&e);
    details::reply(res, r, true);
    r.release_execute_query();
    e.release_record_meta();
}

}

class service : public tateyama::api::server::service {
public:
    service() = default;

    explicit service(jogasaki::api::database& db);

    tateyama::status operator()(
        std::shared_ptr<tateyama::api::server::request const> req,
        std::shared_ptr<tateyama::api::server::response> res
    ) override;

    tateyama::status initialize(tateyama::api::environment& env, void* context) override;

    tateyama::status shutdown() override;

    static std::shared_ptr<service> create() {
        return std::make_shared<service>();
    }
private:

    struct callback_control {
        explicit callback_control(std::shared_ptr<tateyama::api::server::response> response) :
            response_(std::move(response))
        {};
        std::shared_ptr<tateyama::api::server::response> response_{};  //NOLINT
        std::unique_ptr<details::channel_info> channel_info_{};  //NOLINT
    };

    jogasaki::api::database* db_{};
    tbb::concurrent_hash_map<void*, std::shared_ptr<callback_control>> callbacks_{};

    void execute_statement(
        std::shared_ptr<tateyama::api::server::response>& res,
        std::shared_ptr<jogasaki::api::executable_statement> stmt,
        jogasaki::api::transaction_handle tx
    );
    void execute_query(
        std::shared_ptr<tateyama::api::server::response> res,
        details::query_info const& q,
        jogasaki::api::transaction_handle tx
    );
    void set_params(::request::ParameterSet const& ps, std::unique_ptr<jogasaki::api::parameter_set>& params);
    [[nodiscard]] std::size_t new_resultset_id() const noexcept;
};

inline jogasaki::api::impl::service& get_impl(tateyama::api::server::service& svc) {
    return unsafe_downcast<jogasaki::api::impl::service>(svc);
}

}

