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

#include <any>
#include <sstream>
#include <vector>
#include <boost/dynamic_bitset.hpp>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include "jogasaki/proto/sql/common.pb.h"
#include "jogasaki/proto/sql/request.pb.h"
#include "jogasaki/proto/sql/response.pb.h"
#include <jogasaki/api.h>
#include <jogasaki/api/impl/map_error_code.h>
#include <jogasaki/lob/blob_locator.h>
#include <jogasaki/lob/clob_locator.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/utils/convert_offset.h>
#include <jogasaki/utils/decimal.h>

namespace jogasaki::utils {

using namespace std::string_view_literals;
using namespace std::literals::string_literals;
using namespace jogasaki;

using utils::time_of_day_tz;
using utils::time_point_tz;

namespace sql = jogasaki::proto::sql;

using takatori::util::unsafe_downcast;
using takatori::util::maybe_shared_ptr;
std::string serialize(sql::request::Request& r);
void deserialize(std::string_view s, sql::response::Response& res);
struct column_info;

template <class T>
struct allow_arbitrary {
    allow_arbitrary() = default;

    allow_arbitrary(T&& t) :
        entity_(std::forward<T>(t)),
        arbitrary_(false)
    {}
    bool is_arbitrary() const noexcept{
        return arbitrary_;
    }

    T value() const noexcept {
        return entity_;
    }
private:

    T entity_{};
    bool arbitrary_{true};
};

struct colinfo {
    colinfo(
        std::string name,
        sql::common::AtomType type,
        std::optional<bool> nullable
    ) :
        name_(std::move(name)), type_(type), nullable_(nullable)
    {}

    std::string name_{};  //NOLINT
    sql::common::AtomType type_{};  //NOLINT
    std::optional<bool> nullable_{};  //NOLINT
    std::optional<bool> varying_{};
    std::optional<allow_arbitrary<std::uint32_t>> length_{};
    std::optional<allow_arbitrary<std::uint32_t>> precision_{};
    std::optional<allow_arbitrary<std::uint32_t>> scale_{};

};

inline jogasaki::meta::record_meta create_record_meta(std::vector<colinfo> const& columns) {
    std::vector<meta::field_type> fields{};
    boost::dynamic_bitset<std::uint64_t> nullities;
    for(std::size_t i=0, n=columns.size(); i<n; ++i) {
        auto& c = columns[i];
        bool nullable = ! c.nullable_.has_value() || c.nullable_.value(); // currently assume nullable if no info. provided
        meta::field_type field{};
        nullities.push_back(nullable);
        switch(c.type_) {
            using kind = meta::field_type_kind;
            case sql::common::AtomType::BOOLEAN: fields.emplace_back(meta::field_enum_tag<kind::boolean>); break;
            case sql::common::AtomType::INT4: fields.emplace_back(meta::field_enum_tag<kind::int4>); break;
            case sql::common::AtomType::INT8: fields.emplace_back(meta::field_enum_tag<kind::int8>); break;
            case sql::common::AtomType::FLOAT4: fields.emplace_back(meta::field_enum_tag<kind::float4>); break;
            case sql::common::AtomType::FLOAT8: fields.emplace_back(meta::field_enum_tag<kind::float8>); break;
            case sql::common::AtomType::DECIMAL: {
                auto p = c.precision_.has_value() ?
                    (! c.precision_.value().is_arbitrary() ? std::optional<std::size_t>{c.precision_.value().value()} : std::nullopt) :
                    std::nullopt;
                auto s = c.scale_.has_value() ?
                    (! c.scale_.value().is_arbitrary() ? std::optional<std::size_t>{c.scale_.value().value()} : std::nullopt) :
                    std::nullopt;
                fields.emplace_back(std::make_shared<meta::decimal_field_option>(p, s));
                break;
            }
            case sql::common::AtomType::CHARACTER: {
                // if varying info is not provided, assume it is non varying - this is to test varying=true is passed correctly
                auto varying = c.varying_.has_value() && c.varying_.has_value();
                auto length = c.length_.has_value() ?
                    (! c.length_.value().is_arbitrary() ? std::optional<std::size_t>{c.length_.value().value()} : std::nullopt) :
                    std::nullopt;
                fields.emplace_back(std::make_shared<meta::character_field_option>(varying, length));
                break;
            }
            case sql::common::AtomType::OCTET: {
                // if varying info is not provided, assume it is non varying - this is to test varying=true is passed correctly
                auto varying = c.varying_.has_value() && c.varying_.has_value();
                auto length = c.length_.has_value() ?
                    (! c.length_.value().is_arbitrary() ? std::optional<std::size_t>{c.length_.value().value()} : std::nullopt) :
                    std::nullopt;
                fields.emplace_back(std::make_shared<meta::octet_field_option>(varying, length));
                break;
            }
            case sql::common::AtomType::DATE: fields.emplace_back(meta::field_enum_tag<kind::date>); break;
            case sql::common::AtomType::TIME_OF_DAY: fields.emplace_back(std::make_shared<meta::time_of_day_field_option>(false)); break;
            case sql::common::AtomType::TIME_OF_DAY_WITH_TIME_ZONE: fields.emplace_back(std::make_shared<meta::time_of_day_field_option>(true)); break;
            case sql::common::AtomType::TIME_POINT: fields.emplace_back(std::make_shared<meta::time_point_field_option>(false)); break;
            case sql::common::AtomType::TIME_POINT_WITH_TIME_ZONE: fields.emplace_back(std::make_shared<meta::time_point_field_option>(true)); break;
            case sql::common::AtomType::BLOB: fields.emplace_back(meta::field_enum_tag<kind::blob>); break;
            case sql::common::AtomType::CLOB: fields.emplace_back(meta::field_enum_tag<kind::clob>); break;
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

inline void deserialize(std::string_view s, sql::request::Request& req) {
    if (! req.ParseFromString(std::string(s))) {
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
    api::transaction_handle handle_{};
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
        LOG(ERROR) << "**** error returned in Begin : " << err.code() << "'" << err.detail() << "' **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {};
    }
    auto& s = begin.success();
    return {api::transaction_handle{s.transaction_handle().handle()}, s.transaction_id().id()};
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
        LOG(ERROR) << "**** error returned in Prepare : " << sql::error::Code_Name(err.code()) << " '" << err.detail() << "' **** ";
        if (utils_raise_exception_on_error) std::abort();
        return -1;
    }
    auto& stmt = prep.prepared_statement_handle();
    return stmt.handle();
}

inline std::string encode_commit(
    api::transaction_handle tx_handle,
    bool auto_dispose_on_commit_success
) {
    sql::request::Request r{};
    auto cm = r.mutable_commit();
    auto h = cm->mutable_transaction_handle();
    h->set_handle(tx_handle.surrogate_id());
    cm->set_auto_dispose(auto_dispose_on_commit_success);
    return serialize(r);
}

inline std::string encode_rollback(api::transaction_handle tx_handle) {
    sql::request::Request r{};
    auto h = r.mutable_rollback()->mutable_transaction_handle();
    h->set_handle(tx_handle.surrogate_id());
    return serialize(r);
}

struct error {
    error() = default;

    error(error_code code, std::string_view msg) noexcept :
        code_(code),
        message_(msg)
    {}

    error(error_code code, std::string_view msg, std::string_view supplemental_text) noexcept :
        code_(code),
        message_(msg),
        supplemental_text_(supplemental_text)
    {}

    error_code code_;
    std::string message_;
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
        return {false, {api::impl::map_error(er.code()), er.detail(), er.supplemental_text()}};
    }
    return {true, {}};
}

inline std::shared_ptr<request_statistics> make_stats(::jogasaki::proto::sql::response::ExecuteResult::Success const& s) {
    auto ret = std::make_shared<request_statistics>();
    for(auto&& e : s.counters()) {
        switch(e.type()) {
            case ::jogasaki::proto::sql::response::ExecuteResult::INSERTED_ROWS: ret->counter(counter_kind::inserted).count(e.value()); break;
            case ::jogasaki::proto::sql::response::ExecuteResult::UPDATED_ROWS: ret->counter(counter_kind::updated).count(e.value()); break;
            case ::jogasaki::proto::sql::response::ExecuteResult::MERGED_ROWS: ret->counter(counter_kind::merged).count(e.value()); break;
            case ::jogasaki::proto::sql::response::ExecuteResult::DELETED_ROWS: ret->counter(counter_kind::deleted).count(e.value()); break;
            default: break;
        }
    }
    return ret;
}

inline std::tuple<bool, error, std::shared_ptr<request_statistics>> decode_execute_result(std::string_view res) {
    sql::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_execute_result())  {
        LOG(ERROR) << "**** missing execute_result **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {false, {}, {}};
    }
    auto& er = resp.execute_result();
    if (er.has_error()) {
        auto& err = er.error();
        return {false, {api::impl::map_error(err.code()), err.detail(), err.supplemental_text()}, {}};
    }
    return {true, {}, make_stats(er.success())};
}

inline std::string encode_dispose_prepare(std::uint64_t handle) {
    sql::request::Request r{};
    r.mutable_dispose_prepared_statement()->mutable_prepared_statement_handle()->set_handle(handle);
    return serialize(r);
}

template<class T>
std::string encode_execute_statement_or_query(api::transaction_handle tx_handle, std::string_view sql) {
    sql::request::Request r{};
    T* stmt{};
    if constexpr (std::is_same_v<T, sql::request::ExecuteQuery>) {
        stmt = r.mutable_execute_query();
    } else if constexpr (std::is_same_v<T, sql::request::ExecuteStatement>) {
        stmt = r.mutable_execute_statement();
    } else {
        std::abort();
    }
    auto h = stmt->mutable_transaction_handle();
    h->set_handle(tx_handle.surrogate_id());
    stmt->mutable_sql()->assign(sql);
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

inline std::string encode_execute_statement(api::transaction_handle tx_handle, std::string_view sql) {
    return encode_execute_statement_or_query<sql::request::ExecuteStatement>(tx_handle, sql);
}
inline std::string encode_execute_query(api::transaction_handle tx_handle, std::string_view sql) {
    return encode_execute_statement_or_query<sql::request::ExecuteQuery>(tx_handle, sql);
}

template <class T>
std::vector<colinfo> create_colinfo(T& meta) {
    std::vector<colinfo> cols{};
    std::size_t sz = meta.columns_size();
    cols.reserve(sz);
    for(std::size_t i=0; i < sz; ++i) {
        auto& c = meta.columns(i);
        colinfo info{c.name(), c.atom_type(), c.nullable_opt_case() == sql::common::Column::kNullable ? std::optional{c.nullable()} : std::nullopt};
        switch(c.atom_type()) {
            case sql::common::AtomType::DECIMAL: {
                if(c.precision_opt_case() == sql::common::Column::kPrecision) {
                    info.precision_ = c.precision();
                } else if (c.precision_opt_case() == sql::common::Column::kArbitraryPrecision) {
                    info.precision_ = allow_arbitrary<std::uint32_t>{};
                }
                if (c.scale_opt_case() == sql::common::Column::kScale) {
                    info.scale_ = c.scale();
                } else if (c.scale_opt_case() == sql::common::Column::kArbitraryScale) {
                    info.scale_ = allow_arbitrary<std::uint32_t>{};
                }
                break;
            }
            case sql::common::AtomType::CHARACTER: {
                if (c.varying_opt_case() == sql::common::Column::kVarying) {
                    info.varying_ = c.varying();
                }
                break;
            }
            case sql::common::AtomType::OCTET: {
                if (c.varying_opt_case() == sql::common::Column::kVarying) {
                    info.varying_ = c.varying();
                }
                break;
            }
            default:
                break;
        }
        cols.emplace_back(std::move(info));
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
            case ValueCase::kBooleanValue: c0->set_boolean_value(std::any_cast<std::int8_t>(p.value_) != 0); break;
            case ValueCase::kInt4Value: c0->set_int4_value(std::any_cast<std::int32_t>(p.value_)); break;
            case ValueCase::kInt8Value: c0->set_int8_value(std::any_cast<std::int64_t>(p.value_)); break;
            case ValueCase::kFloat4Value: c0->set_float4_value(std::any_cast<float>(p.value_)); break;
            case ValueCase::kFloat8Value: c0->set_float8_value(std::any_cast<double>(p.value_)); break;
            case ValueCase::kCharacterValue: c0->set_character_value(std::any_cast<std::string>(p.value_)); break;
            case ValueCase::kOctetValue: c0->set_octet_value(std::any_cast<std::string>(p.value_)); break;
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
                auto tod = std::any_cast<time_of_day_tz>(p.value_).first;
                auto offset = std::any_cast<time_of_day_tz>(p.value_).second;
                v->set_offset_nanoseconds(tod.time_since_epoch().count());
                v->set_time_zone_offset(offset);
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
                auto* v = c0->mutable_time_point_with_time_zone_value();
                auto tp = std::any_cast<time_point_tz>(p.value_).first;
                auto offset = std::any_cast<time_point_tz>(p.value_).second;
                v->set_offset_seconds(tp.seconds_since_epoch().count());
                v->set_nano_adjustment(tp.subsecond().count());
                v->set_time_zone_offset(offset);
                break;
            }
            case ValueCase::kBlob: {
                auto loc = std::any_cast<lob::blob_locator>(p.value_);
                auto* b = c0->mutable_blob();
                b->mutable_local_path()->assign(loc.path());
                // for convenience, we use the path string as channel name as well
                b->mutable_channel_name()->assign(loc.path());
                break;
            }
            case ValueCase::kClob: {
                auto loc = std::any_cast<lob::clob_locator>(p.value_);
                auto* c = c0->mutable_clob();
                c->mutable_local_path()->assign(loc.path());
                // for convenience, we use the path string as channel name as well
                c->mutable_channel_name()->assign(loc.path());
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
 * @param tx_handle the tx handle used for execute. Specify default constructed transaction handle not to include tx in the msg (non-transactional operation)
 * @param stmt_handle the statement handle to execute
 * @param parameters parameters to fill
 * @param args
 * @return the encoded message
 */
template<class T, class ...Args>
std::string encode_execute_prepared_statement_or_query(
    api::transaction_handle tx_handle,
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
    if(tx_handle) {
        auto h = stmt->mutable_transaction_handle();
        h->set_handle(tx_handle.surrogate_id());
    }
    stmt->mutable_prepared_statement_handle()->set_handle(stmt_handle);
    auto* params = stmt->mutable_parameters();
    fill_parameters(parameters, params);

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

inline std::string encode_execute_prepared_statement(api::transaction_handle tx_handle, std::uint64_t stmt_handle, std::vector<parameter> const& parameters) {
    return encode_execute_prepared_statement_or_query<sql::request::ExecutePreparedStatement>(tx_handle, stmt_handle, parameters);
}
inline std::string encode_execute_prepared_query(api::transaction_handle tx_handle, std::uint64_t stmt_handle, std::vector<parameter> const& parameters) {
    return encode_execute_prepared_statement_or_query<sql::request::ExecutePreparedQuery>(tx_handle, stmt_handle, parameters);
}
template <class ... Args>
std::string encode_execute_dump(api::transaction_handle tx_handle, std::uint64_t stmt_handle, std::vector<parameter> const& parameters, Args...args) {
    return encode_execute_prepared_statement_or_query<sql::request::ExecuteDump>(tx_handle, stmt_handle, parameters, args...);
}

template <class ... Args>
std::string encode_execute_load(api::transaction_handle tx_handle, std::uint64_t stmt_handle, std::vector<parameter> const& parameters, Args...args) {
    return encode_execute_prepared_statement_or_query<sql::request::ExecuteLoad>(tx_handle, stmt_handle, parameters, args...);
}

inline std::string encode_explain(std::uint64_t stmt_handle, std::vector<parameter> const& parameters) {
    sql::request::Request r{};
    auto* explain = r.mutable_explain();
    explain->mutable_prepared_statement_handle()->set_handle(stmt_handle);
    auto* params = explain->mutable_parameters();
    fill_parameters(parameters, params);

    auto s = serialize(r);
    r.clear_explain();
    return s;
}

inline std::string encode_explain_by_text(std::string_view sql) {
    sql::request::Request r{};
    auto* explain = r.mutable_explain_by_text();
    explain->mutable_sql()->assign(sql);

    auto s = serialize(r);
    r.clear_explain_by_text();
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
        return {{}, {}, {}, {}, {api::impl::map_error(er.code()), er.detail()}};
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
    auto s = serialize(r);
    r.clear_describe_table();
    return s;
}

inline std::string encode_list_tables() {
    sql::request::Request r{};
    auto* lt = r.mutable_listtables();
    (void) lt;
    auto s = serialize(r);
    r.clear_listtables();
    return s;
}

inline std::string encode_get_search_path() {
    sql::request::Request r{};
    auto* lt = r.mutable_getsearchpath();
    (void) lt;
    auto s = serialize(r);
    r.clear_getsearchpath();
    return s;
}

inline std::string encode_batch() {
    // currently empty TODO
    sql::request::Request r{};
    auto* bt = r.mutable_batch();
    (void) bt;
    auto s = serialize(r);
    r.clear_batch();
    return s;
}

inline std::string encode_get_error_info(api::transaction_handle tx_handle) {
    sql::request::Request r{};
    auto h = r.mutable_get_error_info()->mutable_transaction_handle();
    h->set_handle(tx_handle.surrogate_id());
    return serialize(r);
}

inline std::string encode_dispose_transaction(api::transaction_handle tx_handle) {
    sql::request::Request r{};
    auto h = r.mutable_dispose_transaction()->mutable_transaction_handle();
    h->set_handle(tx_handle.surrogate_id());
    return serialize(r);
}

struct column_info {
    column_info(
        std::string_view name,
        sql::common::AtomType atom_type,
        std::string_view description = {}
    ) :
        name_(name),
        atom_type_(atom_type),
        description_(description)
    {}
    std::string name_{};
    sql::common::AtomType atom_type_{};
    std::string description_{};
};

struct table_info {
    std::string database_name_{};
    std::string schema_name_{};
    std::string table_name_{};
    std::vector<column_info> columns_{};
    std::string description_{};
    std::vector<std::string> primary_key_columns_{};
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
        return {{}, {api::impl::map_error(er.code()), er.detail()}};
    }

    std::vector<column_info> cols{};
    for(auto&& c : dt.success().columns()) {
        cols.emplace_back(c.name(), c.atom_type(), c.description());
    }

    std::vector<std::string> primary_key_columns{};
    primary_key_columns.reserve(dt.success().primary_key_size());
    for(auto&& p : dt.success().primary_key()) {
        primary_key_columns.emplace_back(p);
    }

    table_info info{
        dt.success().database_name(),
        dt.success().schema_name(),
        dt.success().table_name(),
        std::move(cols),
        dt.success().description(),
        std::move(primary_key_columns)
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
        return {true, {}};
    }
    if(! gei.has_success()) {
        auto& err = gei.error();
        return {false, { api::impl::map_error(err.code()), err.detail(), err.supplemental_text() }};
    }
    auto& err = gei.success();
    return {true, { api::impl::map_error(err.code()), err.detail(), err.supplemental_text() }};
}

inline std::string encode_extract_statement_info(std::string_view payload, std::optional<std::size_t> session_id = {}) {
    sql::request::Request r{};
    auto* extract = r.mutable_extract_statement_info();
    extract->mutable_payload()->assign(payload);
    if(session_id.has_value()) {
        extract->set_session_id(session_id.value());
    }

    auto s = serialize(r);
    r.clear_extract_statement_info();
    return s;
}

inline std::tuple<std::string, std::string, error> decode_extract_statement_info(std::string_view res) {
    sql::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_extract_statement_info())  {
        LOG(ERROR) << "**** missing extract_statement_info **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {{}, {}, {}};
    }
    auto& extract = resp.extract_statement_info();
    if (extract.has_error()) {
        auto& er = extract.error();
        return {{}, {}, {api::impl::map_error(er.code()), er.detail()}};
    }
    return {extract.success().sql(), extract.success().transaction_id().id(), {}};
}

inline std::string encode_get_large_object_data(std::uint64_t id) {
    sql::request::Request r{};
    auto* gd = r.mutable_get_large_object_data();
    auto* ref = gd->mutable_reference();
    ref->set_object_id(id);
    ref->set_provider(sql::common::LargeObjectProvider::DATASTORE);

    auto s = serialize(r);
    r.clear_get_large_object_data();
    return s;
}

inline std::tuple<std::string, std::string, error> decode_get_large_object_data(std::string_view res) {
    sql::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_get_large_object_data())  {
        LOG(ERROR) << "**** missing get_large_object_data **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {{}, {}, {}};
    }
    auto& gd = resp.get_large_object_data();
    if (gd.has_error()) {
        auto& er = gd.error();
        return {{}, {}, {api::impl::map_error(er.code()), er.detail()}};
    }
    auto& s = gd.success();
    if (s.data_case() == proto::sql::response::GetLargeObjectData_Success::DataCase::kChannelName) {
        return {s.channel_name(), {}, {}};
    }
    if (s.data_case() == proto::sql::response::GetLargeObjectData_Success::DataCase::kContents) {
        auto c = s.contents();
        return {{}, {s.contents()}, {}};
    }
    std::abort();
}

inline std::string encode_get_transaction_status(api::transaction_handle tx_handle) {
    sql::request::Request r{};
    auto* gts = r.mutable_get_transaction_status();
    auto* th = gts->mutable_transaction_handle();
    th->set_handle(tx_handle.surrogate_id());

    auto s = serialize(r);
    r.clear_get_transaction_status();
    return s;
}

inline std::tuple<::jogasaki::proto::sql::response::TransactionStatus, std::string, error> decode_get_transaction_status(std::string_view res) {
    sql::response::Response resp{};
    deserialize(res, resp);
    if (! resp.has_get_transaction_status())  {
        LOG(ERROR) << "**** missing get_transaction_status **** ";
        if (utils_raise_exception_on_error) std::abort();
        return {{}, {}, {}};
    }
    auto& gts = resp.get_transaction_status();
    if (gts.has_error()) {
        auto& er = gts.error();
        return {{}, {}, {api::impl::map_error(er.code()), er.detail()}};
    }
    auto& s = gts.success();
    return {s.status(), s.message(), {}};
}

}  // namespace jogasaki::utils
