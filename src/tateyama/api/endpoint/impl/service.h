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
#include <memory>

#include <takatori/util/downcast.h>

#include <jogasaki/api/database.h>

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

class Cursor {
public:
    void clear() {
        result_set_ = nullptr;
        iterator_ = nullptr;
        prepared_ = nullptr;
    }
    std::unique_ptr<jogasaki::api::result_set> result_set_{};  //NOLINT
    std::unique_ptr<jogasaki::api::result_set_iterator> iterator_{};  //NOLINT
    std::unique_ptr<jogasaki::api::prepared_statement> prepared_{};  //NOLINT
    std::string wire_name_;  //NOLINT
    tateyama::api::endpoint::writer* writer_{};  //NOLINT
};

class service : public api::endpoint::service {
public:
    service() = default;

    explicit service(jogasaki::api::database& db) :
        db_(std::addressof(db))
    {
        (void)id_;
    }

    tateyama::status operator()(
        std::shared_ptr<tateyama::api::endpoint::request const> req,
        std::shared_ptr<tateyama::api::endpoint::response> res
    ) override;

private:
    jogasaki::api::database* db_{};

    std::size_t id_{};
    std::unique_ptr<jogasaki::api::transaction> transaction_{};
    std::size_t transaction_id_{};
    std::size_t resultset_id_{};
    std::vector<Cursor> cursors_{};

    tateyama::api::endpoint::data_channel* channel_{};

    [[nodiscard]] const char* execute_statement(std::string_view);
    [[nodiscard]] const char* execute_query(
        tateyama::api::endpoint::response& res,
        std::string_view,
        std::size_t
    );
    void next(std::size_t);
    [[nodiscard]] const char* execute_prepared_statement(std::size_t, jogasaki::api::parameter_set&);
    [[nodiscard]] const char* execute_prepared_query(
        tateyama::api::endpoint::response& res,
        std::size_t, jogasaki::api::parameter_set&, std::size_t
    );
    void set_metadata(std::size_t, schema::RecordMeta&);
    void set_params(::request::ParameterSet const&, std::unique_ptr<jogasaki::api::parameter_set>&);
    void clear_transaction() {
        cursors_.clear();
        transaction_ = nullptr;
    }
    void clear_all() {
        clear_transaction();
    }

    void release_writers(tateyama::api::endpoint::response& res, Cursor& cursor);
    void reply(endpoint::response& res, ::response::Response &r);

    template<typename T>
    void error(endpoint::response&, std::string) {}  //NOLINT(performance-unnecessary-value-param)
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

inline api::endpoint::impl::service& get_impl(api::endpoint::service& svc) {
    return unsafe_downcast<api::endpoint::impl::service>(svc);
}

}

