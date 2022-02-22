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

#include <sstream>
#include <vector>
#include <any>

#include <boost/dynamic_bitset.hpp>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"
#include "schema.pb.h"
#include "status.pb.h"

#include <jogasaki/api.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::utils {

using namespace std::string_view_literals;
using namespace std::literals::string_literals;
using namespace jogasaki;

using takatori::util::unsafe_downcast;
using takatori::util::maybe_shared_ptr;
std::string serialize(::request::Request& r);
void deserialize(std::string_view s, ::response::Response& res);

struct colinfo {
    colinfo(std::string name, ::common::DataType type, bool nullable) :
        name_(std::move(name)), type_(type), nullable_(nullable)
    {}

    std::string name_{};  //NOLINT
    ::common::DataType type_{};  //NOLINT
    bool nullable_{};  //NOLINT
};

inline jogasaki::meta::record_meta create_record_meta(std::vector<colinfo> const& columns) {
    std::vector<meta::field_type> fields{};
    boost::dynamic_bitset<std::uint64_t> nullities;
    for(std::size_t i=0, n=columns.size(); i<n; ++i) {
        auto& c = columns[i];
        bool nullable = c.nullable_;
        meta::field_type field{};
        nullities.push_back(nullable);
        switch(c.type_) {
            using kind = meta::field_type_kind;
            case ::common::DataType::INT4: fields.emplace_back(meta::field_enum_tag<kind::int4>); break;
            case ::common::DataType::INT8: fields.emplace_back(meta::field_enum_tag<kind::int8>); break;
            case ::common::DataType::FLOAT4: fields.emplace_back(meta::field_enum_tag<kind::float4>); break;
            case ::common::DataType::FLOAT8: fields.emplace_back(meta::field_enum_tag<kind::float8>); break;
            case ::common::DataType::CHARACTER: fields.emplace_back(meta::field_enum_tag<kind::character>); break;
            default: std::abort();
        }
    }
    jogasaki::meta::record_meta meta{std::move(fields), std::move(nullities)};
    return meta;
}

inline jogasaki::meta::record_meta create_record_meta(::schema::RecordMeta const& proto) {
    std::vector<meta::field_type> fields{};
    boost::dynamic_bitset<std::uint64_t> nullities;
    for(std::size_t i=0, n=proto.columns_size(); i<n; ++i) {
        auto& c = proto.columns(i);
        bool nullable = c.nullable();
        meta::field_type field{};
        nullities.push_back(nullable);
        switch(c.type()) {
            using kind = meta::field_type_kind;
            case ::common::DataType::INT4: fields.emplace_back(meta::field_enum_tag<kind::int4>); break;
            case ::common::DataType::INT8: fields.emplace_back(meta::field_enum_tag<kind::int8>); break;
            case ::common::DataType::FLOAT4: fields.emplace_back(meta::field_enum_tag<kind::float4>); break;
            case ::common::DataType::FLOAT8: fields.emplace_back(meta::field_enum_tag<kind::float8>); break;
            case ::common::DataType::CHARACTER: fields.emplace_back(meta::field_enum_tag<kind::character>); break;
            default: std::abort();
        }
    }
    jogasaki::meta::record_meta meta{std::move(fields), std::move(nullities)};
    return meta;
}

inline bool utils_raise_exception_on_error = false;

inline std::string serialize(::request::Request& r) {
    std::string s{};
    if (!r.SerializeToString(&s)) {
        std::abort();
    }
//    std::cout << " DebugString : " << r.DebugString() << std::endl;
//    std::cout << " Binary data : " << utils::binary_printer{s.data(), s.size()} << std::endl;
    return s;
}

inline void deserialize(std::string_view s, ::response::Response& res) {
    if (!res.ParseFromString(std::string(s))) {
        std::abort();
    }
//    std::cout << " Binary data : " << utils::binary_printer{s.data(), s.size()} << std::endl;
//    std::cout << " DebugString : " << res.DebugString() << std::endl;
}

inline std::string encode_prepare_vars(
    std::string sql,
    std::unordered_map<std::string, ::common::DataType> const& place_holders
) {
    ::request::Request r{};
    auto* p = r.mutable_prepare();
    p->mutable_sql()->assign(sql);
    if (! place_holders.empty()) {
        auto vars = p->mutable_host_variables();
        for(auto&& [n, t] : place_holders) {
            auto* v = vars->add_variables();
            v->set_name(n);
            v->set_type(t);
        }
    }
    return serialize(r);
}

template <class ...Args>
std::string encode_prepare(std::string sql, Args...args) {
    std::unordered_map<std::string, ::common::DataType> place_holders{args...};
    return encode_prepare_vars(std::move(sql), place_holders);
}

inline std::string encode_begin(
    bool readonly,
    bool is_long = false,
    std::vector<std::string> const& write_preserves = {}
) {
    ::request::Request r{};
    auto opt = r.mutable_begin()->mutable_option();
    opt->set_type(::request::TransactionOption_TransactionType::TransactionOption_TransactionType_TRANSACTION_TYPE_SHORT);
    if(readonly) {
        opt->set_type(::request::TransactionOption_TransactionType::TransactionOption_TransactionType_TRANSACTION_TYPE_READ_ONLY);
    }
    r.mutable_session_handle()->set_handle(1);
    if(is_long) {
        opt->set_type(::request::TransactionOption_TransactionType::TransactionOption_TransactionType_TRANSACTION_TYPE_LONG);
        for(auto&& s : write_preserves) {
            auto* wp = opt->add_write_preserves();
            wp->set_name(s);
        }
    }
    auto s = serialize(r);
    return s;
}

inline std::uint64_t decode_begin(std::string_view res) {
    ::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_begin()) {
        LOG(ERROR) << "**** missing begin msg **** ";
        if (utils_raise_exception_on_error) std::abort();
        return -1;
    }
    auto& begin = resp.begin();
    if (! begin.has_transaction_handle()) {
        auto& err = begin.error();
        LOG(ERROR) << "**** error returned in Begin : " << err.status() << "'" << err.detail() << "' **** ";
        if (utils_raise_exception_on_error) std::abort();
        return -1;
    }
    auto& tx = begin.transaction_handle();
    return tx.handle();
}

inline std::uint64_t decode_prepare(std::string_view res) {
    ::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_prepare()) {
        LOG(ERROR) << "**** missing prepare msg **** ";
        if (utils_raise_exception_on_error) std::abort();
        return -1;
    }
    auto& prep = resp.prepare();
    if (! prep.has_prepared_statement_handle()) {
        auto& err = prep.error();
        LOG(ERROR) << "**** error returned in Prepare : " << ::status::Status_Name(err.status()) << " '" << err.detail() << "' **** ";
        if (utils_raise_exception_on_error) std::abort();
        return -1;
    }
    auto& stmt = prep.prepared_statement_handle();
    return stmt.handle();
}

inline std::string encode_commit(std::uint64_t handle) {
    ::request::Request r{};
    r.mutable_commit()->mutable_transaction_handle()->set_handle(handle);
    return serialize(r);
}

inline std::string encode_rollback(std::uint64_t handle) {
    ::request::Request r{};
    r.mutable_rollback()->mutable_transaction_handle()->set_handle(handle);
    return serialize(r);
}

struct error {
    ::status::Status status_;
    std::string message_;
};

inline std::pair<bool, error> decode_result_only(std::string_view res) {
    ::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_result_only())  {
        LOG(ERROR) << "**** missing result_only **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {false, {}};
    }
    auto& ro = resp.result_only();
    if (ro.has_error()) {
        auto& er = ro.error();
        return {false, {er.status(), er.detail()}};
    }
    return {true, {}};
}

inline std::string encode_dispose_prepare(std::uint64_t handle) {
    ::request::Request r{};
    r.mutable_dispose_prepared_statement()->mutable_prepared_statement_handle()->set_handle(handle);
    return serialize(r);
}

inline std::string encode_disconnect() {
    ::request::Request r{};
    r.mutable_disconnect();
    r.mutable_session_handle()->set_handle(1);
    return serialize(r);
}

template<class T>
std::string encode_execute_statement_or_query(std::uint64_t tx_handle, std::string_view sql) {
    ::request::Request r{};
    T* stmt{};
    if constexpr (std::is_same_v<T, ::request::ExecuteQuery>) {
        stmt = r.mutable_execute_query();
    } else if constexpr (std::is_same_v<T, ::request::ExecuteStatement>) {
        stmt = r.mutable_execute_statement();
    } else {
        std::abort();
    }
    stmt->mutable_transaction_handle()->set_handle(tx_handle);
    stmt->mutable_sql()->assign(sql);
    r.mutable_session_handle()->set_handle(1);
    auto s = serialize(r);
    if constexpr (std::is_same_v<T, ::request::ExecuteQuery>) {
        r.clear_execute_query();
    } else if constexpr (std::is_same_v<T, ::request::ExecuteStatement>) {
        r.clear_execute_statement();
    } else {
        std::abort();
    }
    return s;
}

inline std::string encode_execute_statement(std::uint64_t tx_handle, std::string_view sql) {
    return encode_execute_statement_or_query<::request::ExecuteStatement>(tx_handle, sql);
}
inline std::string encode_execute_query(std::uint64_t tx_handle, std::string_view sql) {
    return encode_execute_statement_or_query<::request::ExecuteQuery>(tx_handle, sql);
}

inline std::pair<std::string, std::vector<colinfo>> decode_execute_query(std::string_view res) {
    ::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_execute_query())  {
        LOG(ERROR) << "**** missing result_only **** ";
        if (utils_raise_exception_on_error) std::abort();
    }
    auto& eq = resp.execute_query();
    auto name = eq.name();
    if (! eq.has_record_meta())  {
        LOG(ERROR) << "**** missing record_meta **** ";
        if (utils_raise_exception_on_error) std::abort();
    }
    auto meta = eq.record_meta();
    std::size_t sz = meta.columns_size();
    std::vector<colinfo> cols{};
    for(std::size_t i=0; i < sz; ++i) {
        auto& c = meta.columns(i);
        cols.emplace_back(c.name(), c.type(), c.nullable());
    }
    return {name, std::move(cols)};
}

struct parameter {
    // construct parameter with null
    explicit parameter(std::string name) :
        name_(std::move(name))
    {}

    template <class T>
    parameter(std::string name, ::common::DataType type, T value) :
        name_(std::move(name)), type_(type), value_(value)
    {}
    std::string name_{};
    ::common::DataType type_{};
    std::any value_{};
};

void fill_parameters(std::vector<parameter> const& parameters, ::request::ParameterSet* ps) {
    for(auto&& p : parameters) {
        auto* c0 = ps->add_parameters();
        c0->set_name(p.name_);
        if (! p.value_.has_value()) {
            // null value
            return;
        }
        switch (p.type_) {
            case ::common::DataType::INT4: c0->set_int4_value(std::any_cast<std::int32_t>(p.value_)); break;
            case ::common::DataType::INT8: c0->set_int8_value(std::any_cast<std::int64_t>(p.value_)); break;
            case ::common::DataType::FLOAT4: c0->set_float4_value(std::any_cast<float>(p.value_)); break;
            case ::common::DataType::FLOAT8: c0->set_float8_value(std::any_cast<double>(p.value_)); break;
            case ::common::DataType::CHARACTER: c0->set_character_value(std::any_cast<std::string>(p.value_)); break;
            default: std::abort();
        }
    }
}

template<class T>
std::string encode_execute_prepared_statement_or_query(std::uint64_t tx_handle, std::uint64_t stmt_handle, std::vector<parameter> const& parameters) {
    ::request::Request r{};
    T* stmt{};
    if constexpr (std::is_same_v<T, ::request::ExecutePreparedQuery>) {
        stmt = r.mutable_execute_prepared_query();
    } else if constexpr (std::is_same_v<T, ::request::ExecutePreparedStatement>) {
        stmt = r.mutable_execute_prepared_statement();
    } else {
        std::abort();
    }
    stmt->mutable_transaction_handle()->set_handle(tx_handle);
    stmt->mutable_prepared_statement_handle()->set_handle(stmt_handle);

    auto* params = stmt->mutable_parameters();
    fill_parameters(parameters, params);

    r.mutable_session_handle()->set_handle(1);
    auto s = serialize(r);
    if constexpr (std::is_same_v<T, ::request::ExecutePreparedQuery>) {
        r.clear_execute_prepared_query();
    } else if constexpr (std::is_same_v<T, ::request::ExecutePreparedStatement>) {
        r.clear_execute_prepared_statement();
    } else {
        std::abort();
    }
    return s;
}

inline std::string encode_execute_prepared_statement(std::uint64_t tx_handle, std::uint64_t stmt_handle, std::vector<parameter> const& parameters) {
    return encode_execute_prepared_statement_or_query<::request::ExecutePreparedStatement>(tx_handle, stmt_handle, parameters);
}
inline std::string encode_execute_prepared_query(std::uint64_t tx_handle, std::uint64_t stmt_handle, std::vector<parameter> const& parameters) {
    return encode_execute_prepared_statement_or_query<::request::ExecutePreparedQuery>(tx_handle, stmt_handle, parameters);
}

std::string encode_explain(std::uint64_t stmt_handle, std::vector<parameter> const& parameters) {
    ::request::Request r{};
    auto* explain = r.mutable_explain();
    explain->mutable_prepared_statement_handle()->set_handle(stmt_handle);
    auto* params = explain->mutable_parameters();
    fill_parameters(parameters, params);

    r.mutable_session_handle()->set_handle(1);
    auto s = serialize(r);
    r.clear_explain();
    return s;
}

inline std::pair<std::string, error> decode_explain(std::string_view res) {
    ::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_explain())  {
        LOG(ERROR) << "**** missing explain **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {{}, {}};
    }
    auto& explain = resp.explain();
    if (explain.has_error()) {
        auto& er = explain.error();
        return {{}, {er.status(), er.detail()}};
    }
    return {explain.output(), {}};
}

}
