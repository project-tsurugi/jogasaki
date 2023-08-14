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

#include "jogasaki/proto/sql/request.pb.h"
#include "jogasaki/proto/sql/response.pb.h"
#include "jogasaki/proto/sql/common.pb.h"
#include "jogasaki/proto/sql/status.pb.h"

#include <jogasaki/api.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/decimal.h>
#include <jogasaki/api/impl/map_error_code.h>

namespace jogasaki::utils {

using namespace std::string_view_literals;
using namespace std::literals::string_literals;
using namespace jogasaki;
namespace sql = jogasaki::proto::sql;

using takatori::util::unsafe_downcast;
using takatori::util::maybe_shared_ptr;
std::string serialize(sql::request::Request& r);
void deserialize(std::string_view s, sql::response::Response& res);
struct column_info;

struct colinfo {
    colinfo(std::string name, sql::common::AtomType type, bool nullable) :
        name_(std::move(name)), type_(type), nullable_(nullable)
    {}

    std::string name_{};  //NOLINT
    sql::common::AtomType type_{};  //NOLINT
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
            case sql::common::AtomType::INT4: fields.emplace_back(meta::field_enum_tag<kind::int4>); break;
            case sql::common::AtomType::INT8: fields.emplace_back(meta::field_enum_tag<kind::int8>); break;
            case sql::common::AtomType::FLOAT4: fields.emplace_back(meta::field_enum_tag<kind::float4>); break;
            case sql::common::AtomType::FLOAT8: fields.emplace_back(meta::field_enum_tag<kind::float8>); break;
            case sql::common::AtomType::CHARACTER: fields.emplace_back(meta::field_enum_tag<kind::character>); break;
            case sql::common::AtomType::OCTET: fields.emplace_back(meta::field_enum_tag<kind::octet>); break;
            case sql::common::AtomType::DECIMAL: fields.emplace_back(std::make_shared<meta::decimal_field_option>()); break;
            case sql::common::AtomType::DATE: fields.emplace_back(meta::field_enum_tag<kind::date>); break;
            case sql::common::AtomType::TIME_OF_DAY: fields.emplace_back(std::make_shared<meta::time_of_day_field_option>(false)); break;
            case sql::common::AtomType::TIME_OF_DAY_WITH_TIME_ZONE: fields.emplace_back(std::make_shared<meta::time_of_day_field_option>(true)); break;
            case sql::common::AtomType::TIME_POINT: fields.emplace_back(std::make_shared<meta::time_point_field_option>(false)); break;
            case sql::common::AtomType::TIME_POINT_WITH_TIME_ZONE: fields.emplace_back(std::make_shared<meta::time_point_field_option>(true)); break;
            default: std::abort();
        }
    }
    jogasaki::meta::record_meta meta{std::move(fields), std::move(nullities)};
    return meta;
}

inline jogasaki::meta::record_meta create_record_meta(sql::response::ResultSetMetadata const& proto) {
    std::vector<meta::field_type> fields{};
    boost::dynamic_bitset<std::uint64_t> nullities;
    for(std::size_t i=0, n=proto.columns_size(); i<n; ++i) {
        auto& c = proto.columns(i);
        meta::field_type field{};
        nullities.push_back(true); // TODO assume all nullable
        switch(c.atom_type()) {
            using kind = meta::field_type_kind;
            case sql::common::AtomType::INT4: fields.emplace_back(meta::field_enum_tag<kind::int4>); break;
            case sql::common::AtomType::INT8: fields.emplace_back(meta::field_enum_tag<kind::int8>); break;
            case sql::common::AtomType::FLOAT4: fields.emplace_back(meta::field_enum_tag<kind::float4>); break;
            case sql::common::AtomType::FLOAT8: fields.emplace_back(meta::field_enum_tag<kind::float8>); break;
            case sql::common::AtomType::CHARACTER: fields.emplace_back(meta::field_enum_tag<kind::character>); break;
            case sql::common::AtomType::OCTET: fields.emplace_back(meta::field_enum_tag<kind::octet>); break;
            case sql::common::AtomType::DECIMAL: fields.emplace_back(std::make_shared<meta::decimal_field_option>()); break;
            case sql::common::AtomType::DATE: fields.emplace_back(meta::field_enum_tag<kind::date>); break;
            case sql::common::AtomType::TIME_OF_DAY: fields.emplace_back(std::make_shared<meta::time_of_day_field_option>()); break;
            case sql::common::AtomType::TIME_POINT: fields.emplace_back(std::make_shared<meta::time_point_field_option>()); break;
            default: std::abort();
        }
    }
    jogasaki::meta::record_meta meta{std::move(fields), std::move(nullities)};
    return meta;
}

inline bool utils_raise_exception_on_error = false;

inline std::string serialize(sql::request::Request& r) {
    std::string s{};
    if (!r.SerializeToString(&s)) {
        std::abort();
    }
//    std::cout << " DebugString : " << r.DebugString() << std::endl;
//    std::cout << " Binary data : " << utils::binary_printer{s.data(), s.size()} << std::endl;
    return s;
}

inline void deserialize(std::string_view s, sql::response::Response& res) {
    if (!res.ParseFromString(std::string(s))) {
        std::abort();
    }
//    std::cout << " Binary data : " << utils::binary_printer{s.data(), s.size()} << std::endl;
//    std::cout << " DebugString : " << res.DebugString() << std::endl;
}

inline std::string encode_prepare_vars(
    std::string sql,
    std::unordered_map<std::string, sql::common::AtomType> const& place_holders
) {
    sql::request::Request r{};
    auto* p = r.mutable_prepare();
    p->mutable_sql()->assign(sql);
    if (! place_holders.empty()) {
        auto* vars = p->mutable_placeholders();
        for(auto&& [n, t] : place_holders) {
            auto* ph = vars->Add();
            ph->set_name(n);
            ph->set_atom_type(t);
        }
    }
    return serialize(r);
}

template <class ...Args>
std::string encode_prepare(std::string sql, Args...args) {
    std::unordered_map<std::string, sql::common::AtomType> place_holders{args...};
    return encode_prepare_vars(std::move(sql), place_holders);
}

inline std::string encode_begin(
    bool readonly,
    bool is_long = false,
    std::vector<std::string> const& write_preserves = {},
    std::string_view label = {},
    bool modifies_definitions = false
) {
    sql::request::Request r{};
    auto opt = r.mutable_begin()->mutable_option();
    opt->set_type(sql::request::TransactionType::SHORT);
    if(readonly) {
        opt->set_type(sql::request::TransactionType::READ_ONLY);
    }
    r.mutable_session_handle()->set_handle(1);
    if(is_long) {
        opt->set_type(sql::request::TransactionType::LONG);
        for(auto&& s : write_preserves) {
            auto* wp = opt->add_write_preserves();
            wp->set_table_name(s);
        }
    }
    if(! label.empty()) {
        opt->set_label(label.data(), label.size());
    }
    opt->set_modifies_definitions(modifies_definitions);
    auto s = serialize(r);
    return s;
}

constexpr static std::uint64_t handle_undefined = static_cast<std::uint64_t>(-1);

struct begin_result {
    std::uint64_t handle_{handle_undefined};
    std::string transaction_id_{};
};

inline begin_result decode_begin(std::string_view res) {
    sql::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_begin()) {
        LOG(ERROR) << "**** missing begin msg **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {};
    }
    auto& begin = resp.begin();
    if (! begin.has_success()) {
        auto& err = begin.error();
        LOG(ERROR) << "**** error returned in Begin : " << err.status() << "'" << err.detail() << "' **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {};
    }
    auto& s = begin.success();
    return {s.transaction_handle().handle(), s.transaction_id().id()};
}

inline std::uint64_t decode_prepare(std::string_view res) {
    sql::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_prepare()) {
        LOG(ERROR) << "**** missing prepare msg **** ";
        if (utils_raise_exception_on_error) std::abort();
        return -1;
    }
    auto& prep = resp.prepare();
    if (! prep.has_prepared_statement_handle()) {
        auto& err = prep.error();
        LOG(ERROR) << "**** error returned in Prepare : " << sql::status::Status_Name(err.status()) << " '" << err.detail() << "' **** ";
        if (utils_raise_exception_on_error) std::abort();
        return -1;
    }
    auto& stmt = prep.prepared_statement_handle();
    return stmt.handle();
}

inline std::string encode_commit(std::uint64_t handle) {
    sql::request::Request r{};
    r.mutable_commit()->mutable_transaction_handle()->set_handle(handle);
    return serialize(r);
}

inline std::string encode_rollback(std::uint64_t handle) {
    sql::request::Request r{};
    r.mutable_rollback()->mutable_transaction_handle()->set_handle(handle);
    return serialize(r);
}

struct error {
    error() = default;

    error(sql::status::Status st, std::string_view msg) noexcept :
        status_(st),
        message_(msg)
    {}

    error(error_code code, std::string_view msg) noexcept :
        message_(msg),
        code_(code)
    {}

    error(sql::status::Status st, std::string_view msg, error_code code) noexcept :
        status_(st),
        message_(msg),
        code_(code)
    {}

    error(sql::status::Status st, std::string_view msg, error_code code, std::string_view supplemental_text) noexcept :
        status_(st),
        message_(msg),
        code_(code),
        supplemental_text_(supplemental_text)
    {}

    sql::status::Status status_;
    std::string message_;
    error_code code_;
    std::string supplemental_text_;
};

inline std::pair<bool, error> decode_result_only(std::string_view res) {
    sql::response::Response resp{};
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
    sql::request::Request r{};
    r.mutable_dispose_prepared_statement()->mutable_prepared_statement_handle()->set_handle(handle);
    return serialize(r);
}

template<class T>
std::string encode_execute_statement_or_query(std::uint64_t tx_handle, std::string_view sql) {
    sql::request::Request r{};
    T* stmt{};
    if constexpr (std::is_same_v<T, sql::request::ExecuteQuery>) {
        stmt = r.mutable_execute_query();
    } else if constexpr (std::is_same_v<T, sql::request::ExecuteStatement>) {
        stmt = r.mutable_execute_statement();
    } else {
        std::abort();
    }
    stmt->mutable_transaction_handle()->set_handle(tx_handle);
    stmt->mutable_sql()->assign(sql);
    r.mutable_session_handle()->set_handle(1);
    auto s = serialize(r);
    if constexpr (std::is_same_v<T, sql::request::ExecuteQuery>) {
        r.clear_execute_query();
    } else if constexpr (std::is_same_v<T, sql::request::ExecuteStatement>) {
        r.clear_execute_statement();
    } else {
        std::abort();
    }
    return s;
}

inline std::string encode_execute_statement(std::uint64_t tx_handle, std::string_view sql) {
    return encode_execute_statement_or_query<sql::request::ExecuteStatement>(tx_handle, sql);
}
inline std::string encode_execute_query(std::uint64_t tx_handle, std::string_view sql) {
    return encode_execute_statement_or_query<sql::request::ExecuteQuery>(tx_handle, sql);
}

template <class T>
std::vector<colinfo> create_colinfo(T& meta) {
    std::vector<colinfo> cols{};
    std::size_t sz = meta.columns_size();
    cols.reserve(sz);
    for(std::size_t i=0; i < sz; ++i) {
        auto& c = meta.columns(i);
        cols.emplace_back(c.name(), c.atom_type(), true);  // all nullable
    }
    return cols;
}

inline std::pair<std::string, std::vector<colinfo>> decode_execute_query(std::string_view res) {
    sql::response::Response resp{};
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
    auto cols = create_colinfo(meta);
    return {name, std::move(cols)};
}

struct parameter {
    // construct parameter with null
    explicit parameter(std::string name) :
        name_(std::move(name))
    {}

    template <class T>
    parameter(std::string name, sql::request::Parameter::ValueCase type, T value) :
        name_(std::move(name)), type_(type), value_(value)
    {}
    std::string name_{};
    sql::request::Parameter::ValueCase type_{};
    std::any value_{};
};

inline void fill_parameters(
    std::vector<parameter> const& parameters,
    ::google::protobuf::RepeatedPtrField<sql::request::Parameter>* ps
) {
    for(auto&& p : parameters) {
        auto* c0 = ps->Add();
        c0->set_name(p.name_);
        if (! p.value_.has_value()) {
            // null value
            return;
        }
        using ValueCase = sql::request::Parameter::ValueCase;
        switch (p.type_) {
            case ValueCase::kInt4Value: c0->set_int4_value(std::any_cast<std::int32_t>(p.value_)); break;
            case ValueCase::kInt8Value: c0->set_int8_value(std::any_cast<std::int64_t>(p.value_)); break;
            case ValueCase::kFloat4Value: c0->set_float4_value(std::any_cast<float>(p.value_)); break;
            case ValueCase::kFloat8Value: c0->set_float8_value(std::any_cast<double>(p.value_)); break;
            case ValueCase::kCharacterValue: c0->set_character_value(std::any_cast<std::string>(p.value_)); break;
            case ValueCase::kDecimalValue: {
                auto triple = std::any_cast<takatori::decimal::triple>(p.value_);
                auto* v = c0->mutable_decimal_value();
                auto [hi, lo, sz] = utils::make_signed_coefficient_full(triple);
                utils::decimal_buffer buf{};
                utils::create_decimal(triple.sign(), lo, hi, sz, buf);
                v->set_unscaled_value(buf.data(), sz);
                v->set_exponent(triple.exponent());
                break;
            }
            case ValueCase::kDateValue: c0->set_date_value(std::any_cast<runtime_t<meta::field_type_kind::date>>(p.value_).days_since_epoch()); break;
            case ValueCase::kTimeOfDayValue: {
                c0->set_time_of_day_value(std::any_cast<runtime_t<meta::field_type_kind::time_of_day>>(p.value_).time_since_epoch().count());
                break;
            }
            case ValueCase::kTimeOfDayWithTimeZoneValue: {
                auto* v = c0->mutable_time_of_day_with_time_zone_value();
                v->set_offset_nanoseconds(std::any_cast<runtime_t<meta::field_type_kind::time_of_day>>(p.value_).time_since_epoch().count());
                v->set_time_zone_offset(0);  //TOOD pass correct offset
                break;
            }
            case ValueCase::kTimePointValue: {
                auto tp = std::any_cast<runtime_t<meta::field_type_kind::time_point>>(p.value_);
                auto* v = c0->mutable_time_point_value();
                v->set_offset_seconds(tp.seconds_since_epoch().count());
                v->set_nano_adjustment(tp.subsecond().count());
                break;
            }
            case ValueCase::kTimePointWithTimeZoneValue: {
                auto tp = std::any_cast<runtime_t<meta::field_type_kind::time_point>>(p.value_);
                auto* v = c0->mutable_time_point_with_time_zone_value();
                v->set_offset_seconds(tp.seconds_since_epoch().count());
                v->set_nano_adjustment(tp.subsecond().count());
                v->set_time_zone_offset(0);  //TOOD pass correct offset
                break;
            }
            case ValueCase::kReferenceColumnPosition: c0->set_reference_column_position(std::any_cast<std::uint64_t>(p.value_)); break;
            case ValueCase::kReferenceColumnName: c0->set_reference_column_name(std::any_cast<std::string>(p.value_)); break;
            default: std::abort();
        }
    }
}

/**
 * @brief encode msg for execute
 * @tparam T
 * @tparam Args
 * @param tx_handle the tx handle used for execute. Specify 0 not to include tx in the msg (non-transactional operation)
 * @param stmt_handle the statement handle to execute
 * @param parameters parameters to fill
 * @param args
 * @return the encoded message
 */
template<class T, class ...Args>
std::string encode_execute_prepared_statement_or_query(
    std::uint64_t tx_handle,
    std::uint64_t stmt_handle,
    std::vector<parameter> const& parameters,
    Args...args
) {
    sql::request::Request r{};
    T* stmt{};
    if constexpr (std::is_same_v<T, sql::request::ExecutePreparedQuery>) {
        stmt = r.mutable_execute_prepared_query();
    } else if constexpr (std::is_same_v<T, sql::request::ExecutePreparedStatement>) {
        stmt = r.mutable_execute_prepared_statement();
    } else if constexpr (std::is_same_v<T, sql::request::ExecuteDump>) {
        stmt = r.mutable_execute_dump();
        std::vector<std::string> v{args...};
        static_assert(sizeof...(args)==1);
        stmt->mutable_directory()->assign(v[0]);
    } else if constexpr (std::is_same_v<T, sql::request::ExecuteLoad>) {
        stmt = r.mutable_execute_load();
        std::vector<std::string> v{args...};
        auto f = stmt->mutable_file();
        for(auto&& s : v) {
            f->Add()->assign(s);
        }
    } else {
        std::abort();
    }
    if(tx_handle != 0) {
        stmt->mutable_transaction_handle()->set_handle(tx_handle);
    }
    stmt->mutable_prepared_statement_handle()->set_handle(stmt_handle);
    auto* params = stmt->mutable_parameters();
    fill_parameters(parameters, params);

    r.mutable_session_handle()->set_handle(1);
    auto s = serialize(r);
    if constexpr (std::is_same_v<T, sql::request::ExecutePreparedQuery>) {
        r.clear_execute_prepared_query();
    } else if constexpr (std::is_same_v<T, sql::request::ExecutePreparedStatement>) {
        r.clear_execute_prepared_statement();
    } else if constexpr (std::is_same_v<T, sql::request::ExecuteDump>) {
        r.clear_execute_dump();
    } else if constexpr (std::is_same_v<T, sql::request::ExecuteLoad>) {
        r.clear_execute_load();
    } else {
        std::abort();
    }
    return s;
}

inline std::string encode_execute_prepared_statement(std::uint64_t tx_handle, std::uint64_t stmt_handle, std::vector<parameter> const& parameters) {
    return encode_execute_prepared_statement_or_query<sql::request::ExecutePreparedStatement>(tx_handle, stmt_handle, parameters);
}
inline std::string encode_execute_prepared_query(std::uint64_t tx_handle, std::uint64_t stmt_handle, std::vector<parameter> const& parameters) {
    return encode_execute_prepared_statement_or_query<sql::request::ExecutePreparedQuery>(tx_handle, stmt_handle, parameters);
}
template <class ... Args>
std::string encode_execute_dump(std::uint64_t tx_handle, std::uint64_t stmt_handle, std::vector<parameter> const& parameters, Args...args) {
    return encode_execute_prepared_statement_or_query<sql::request::ExecuteDump>(tx_handle, stmt_handle, parameters, args...);
}

template <class ... Args>
std::string encode_execute_load(std::uint64_t tx_handle, std::uint64_t stmt_handle, std::vector<parameter> const& parameters, Args...args) {
    return encode_execute_prepared_statement_or_query<sql::request::ExecuteLoad>(tx_handle, stmt_handle, parameters, args...);
}

inline std::string encode_explain(std::uint64_t stmt_handle, std::vector<parameter> const& parameters) {
    sql::request::Request r{};
    auto* explain = r.mutable_explain();
    explain->mutable_prepared_statement_handle()->set_handle(stmt_handle);
    auto* params = explain->mutable_parameters();
    fill_parameters(parameters, params);

    r.mutable_session_handle()->set_handle(1);
    auto s = serialize(r);
    r.clear_explain();
    return s;
}


inline std::tuple<std::string, std::string, std::size_t, std::vector<colinfo>, error> decode_explain(std::string_view res) {
    sql::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_explain())  {
        LOG(ERROR) << "**** missing explain **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {{}, {}, {}, {}, {}};
    }
    auto& explain = resp.explain();
    if (explain.has_error()) {
        auto& er = explain.error();
        return {{}, {}, {}, {}, {er.status(), er.detail()}};
    }
    auto cols = create_colinfo(explain.success());
    return {
        explain.success().contents(),
        explain.success().format_id(),
        explain.success().format_version(),
        std::move(cols),
        {}
    };
}

inline std::string encode_describe_table(std::string_view name) {
    sql::request::Request r{};
    auto* dt = r.mutable_describe_table();
    dt->set_name(std::string{name});
    r.mutable_session_handle()->set_handle(1);
    auto s = serialize(r);
    r.clear_describe_table();
    return s;
}

inline std::string encode_list_tables() {
    sql::request::Request r{};
    auto* lt = r.mutable_listtables();
    (void) lt;
    r.mutable_session_handle()->set_handle(1);
    auto s = serialize(r);
    r.clear_listtables();
    return s;
}

inline std::string encode_get_search_path() {
    sql::request::Request r{};
    auto* lt = r.mutable_getsearchpath();
    (void) lt;
    r.mutable_session_handle()->set_handle(1);
    auto s = serialize(r);
    r.clear_getsearchpath();
    return s;
}

inline std::string encode_get_error_info(std::uint64_t handle) {
    sql::request::Request r{};
    r.mutable_get_error_info()->mutable_transaction_handle()->set_handle(handle);
    return serialize(r);
}

inline std::string encode_dispose_transaction(std::uint64_t handle) {
    sql::request::Request r{};
    r.mutable_dispose_transaction()->mutable_transaction_handle()->set_handle(handle);
    return serialize(r);
}

struct column_info {
    column_info(
        std::string_view name,
        sql::common::AtomType atom_type
    ) :
        name_(name),
        atom_type_(atom_type)
    {}
    std::string name_{};
    sql::common::AtomType atom_type_{};
};

struct table_info {
    std::string database_name_{};
    std::string schema_name_{};
    std::string table_name_{};
    std::vector<column_info> columns_{};
};

inline std::pair<table_info, error> decode_describe_table(std::string_view res) {
    sql::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_describe_table())  {
        LOG(ERROR) << "**** missing describe_table **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {{}, {}};
    }
    auto& dt = resp.describe_table();
    if (dt.has_error()) {
        auto& er = dt.error();
        return {{}, {er.status(), er.detail()}};
    }

    std::vector<column_info> cols{};
    for(auto&& c : dt.success().columns()) {
        cols.emplace_back(c.name(), c.atom_type());
    }

    table_info info{
        dt.success().database_name(),
        dt.success().schema_name(),
        dt.success().table_name(),
        std::move(cols)
    };
    return {info, {}};
}

inline std::vector<std::string> decode_list_tables(std::string_view res) {
    sql::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_list_tables())  {
        LOG(ERROR) << "**** missing list_tables **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {};
    }
    auto& lt = resp.list_tables();
    if (lt.has_error()) {
        return {};
    }

    std::vector<std::string> ret{};
    for(auto&& n : lt.success().table_path_names()) {
        // assuming currently simple name only
        ret.emplace_back(n.identifiers().Get(0).label());
    }
    return ret;
}

inline std::vector<std::string> decode_get_search_path(std::string_view res) {
    sql::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_get_search_path())  {
        LOG(ERROR) << "**** missing get_search_path **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {};
    }
    auto& gsp = resp.get_search_path();
    if (gsp.has_error()) {
        return {};
    }

    std::vector<std::string> ret{};
    for(auto&& n : gsp.success().search_paths()) {
        // assuming simple name only
        ret.emplace_back(n.identifiers().Get(0).label());
    }
    return ret;
}

inline std::pair<bool, error> decode_get_error_info(std::string_view res) {
    sql::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_get_error_info())  {
        LOG(ERROR) << "**** missing get_error_info **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {false, {}};
    }
    auto& gei = resp.get_error_info();
    if (gei.has_error_not_found()) {
        return {false, {}};
    }
    if(! gei.has_success()) {
        return {false, {}};
    }
    auto& err = gei.success();
    return {true, { err.status(), err.detail(), api::impl::map_error(err.code()), err.supplemental_text() }};
}
}
