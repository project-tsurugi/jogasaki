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
#include "udf_any_sequence_stream.h"

#include <chrono>
#include <memory>
#include <optional>
#include <sstream>
#include <utility>
#include <vector>

#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/status.h>
#include <jogasaki/udf/data/udf_semantic_type.h>
#include <jogasaki/udf/data/udf_wire_codec.h>
#include <jogasaki/udf/generic_record.h>
#include <jogasaki/udf/generic_record_impl.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/convert_offset.h>

namespace jogasaki::udf::data {

namespace {
using any = ::jogasaki::data::any;

constexpr std::string_view udf_out_prefix = "[udf out] ";

template <class T, class Opt> void emplace_nullable(std::vector<any>& values, Opt const& opt) {
    if (opt) {
        values.emplace_back(std::in_place_type<T>, *opt);
        VLOG_LP(log_trace) << udf_out_prefix << typeid(T).name() << ":" << *opt;
    } else {
        values.emplace_back();
        VLOG_LP(log_trace) << udf_out_prefix << typeid(T).name() << ":NULL";
    }
}

template <class T, class Opt, class F>
void emplace_nullable_with(std::vector<any>& values, Opt const& opt, F const& f) {
    if (opt) {
        values.emplace_back(std::in_place_type<T>, f(*opt));
        VLOG_LP(log_trace) << udf_out_prefix << typeid(T).name() << ":" << *opt;
    } else {
        values.emplace_back();
        VLOG_LP(log_trace) << udf_out_prefix << typeid(T).name() << ":NULL";
    }
}

void append_decimal(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    auto unscaled_opt = cursor.fetch_string();
    auto exponent_opt = cursor.fetch_int4();
    if (unscaled_opt && exponent_opt) {
        if (VLOG_IS_ON(log_trace)) {
            std::string_view bin_view{unscaled_opt->data(), unscaled_opt->size()};
            VLOG_LP(log_trace) << udf_out_prefix << "decimal:(" << utils::binary_printer{bin_view}.show_hyphen(false) << "," << *exponent_opt << ")";
        }
        auto triple = jogasaki::udf::data::decode_decimal_triple(*unscaled_opt, *exponent_opt);
        values.emplace_back(std::in_place_type<runtime_t<meta::field_type_kind::decimal>>, triple);
    } else {
        VLOG_LP(log_trace) << udf_out_prefix << "decimal:NULL";
        values.emplace_back();
    }
}

void append_date(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    if (auto days_opt = cursor.fetch_int4()) {
        VLOG_LP(log_trace) << udf_out_prefix << "date:" << *days_opt;
        auto date = jogasaki::udf::data::decode_date_from_wire(*days_opt);
        values.emplace_back(std::in_place_type<runtime_t<meta::field_type_kind::date>>, date);
    } else {
        VLOG_LP(log_trace) << udf_out_prefix << "date:NULL";
        values.emplace_back();
    }
}

void append_time_of_day(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    if (auto nanos_opt = cursor.fetch_int8()) {
        VLOG_LP(log_trace) << udf_out_prefix << "time_of_day:" << *nanos_opt;
        auto tod = jogasaki::udf::data::decode_time_of_day_from_wire(*nanos_opt);
        values.emplace_back(std::in_place_type<runtime_t<meta::field_type_kind::time_of_day>>, tod);
    } else {
        VLOG_LP(log_trace) << udf_out_prefix << "time_of_day:NULL";
        values.emplace_back();
    }
}

void append_time_point(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    auto sec_opt  = cursor.fetch_int8();
    auto nano_opt = cursor.fetch_uint4();
    if (sec_opt && nano_opt) {
        VLOG_LP(log_trace) << udf_out_prefix << "time_point:(" << *sec_opt << "," << *nano_opt << ")";
        auto tp = jogasaki::udf::data::decode_time_point_from_wire(*sec_opt, *nano_opt);
        values.emplace_back(std::in_place_type<runtime_t<meta::field_type_kind::time_point>>, tp);
    } else {
        VLOG_LP(log_trace) << udf_out_prefix << "time_point:NULL";
        values.emplace_back();
    }
}

void append_time_point_with_time_zone(
    std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    auto sec_opt = cursor.fetch_int8();
    auto nano_opt = cursor.fetch_uint4();
    auto tz_offset = cursor.fetch_int4();
    if (sec_opt && nano_opt && tz_offset) {
        VLOG_LP(log_trace) << udf_out_prefix << "time_point_tz:("
                           << *sec_opt << "," << *nano_opt << "," << *tz_offset << ")";
        auto tp_local = jogasaki::udf::data::decode_time_point_from_wire(*sec_opt, *nano_opt);
        auto offset_min = static_cast<std::int32_t>(*tz_offset);
        auto tp =
            jogasaki::utils::remove_offset(jogasaki::utils::time_point_tz{tp_local, offset_min});

        values.emplace_back(std::in_place_type<runtime_t<meta::field_type_kind::time_point>>, tp);
    } else {
        VLOG_LP(log_trace) << udf_out_prefix << "time_point_tz:NULL";
        values.emplace_back();
    }
}

template <class Ref, class DecodeFn>
void append_lob_reference(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor,
    DecodeFn const& decode_fn) {
    auto storage_opt = cursor.fetch_uint8();
    auto object_opt  = cursor.fetch_uint8();
    auto tag_opt     = cursor.fetch_uint8();
    auto prov_opt    = cursor.fetch_bool();

    if (storage_opt && object_opt && tag_opt) {
        if (VLOG_IS_ON(log_trace)) {
            std::string prov_str = prov_opt ? (prov_opt.value() ? "true" : "false") : "empty";
            VLOG_LP(log_trace) << udf_out_prefix << "lob:("
                               << *storage_opt << "," << *object_opt << "," << *tag_opt << "," << prov_str << ")";
        }
        auto ref = decode_fn(*storage_opt, *object_opt, *tag_opt, prov_opt);
        values.emplace_back(std::in_place_type<Ref>, ref);
    } else {
        VLOG_LP(log_trace) << udf_out_prefix << "lob:NULL";
        values.emplace_back();
    }
}

void append_blob(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    append_lob_reference<runtime_t<meta::field_type_kind::blob>>(values, cursor,
        [](std::uint64_t s, std::uint64_t o, std::uint64_t t, std::optional<bool> p) {
            return jogasaki::udf::data::decode_blob_reference_from_wire(s, o, t, p);
        });
}

void append_clob(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    append_lob_reference<runtime_t<meta::field_type_kind::clob>>(values, cursor,
        [](std::uint64_t s, std::uint64_t o, std::uint64_t t, std::optional<bool> p) {
            return jogasaki::udf::data::decode_clob_reference_from_wire(s, o, t, p);
        });
}

void fail_unsupported() {
    std::ostringstream ss;
    ss << "unsupported meta::field_type in convert_record_to_sequence()";
    fail_with_exception_msg(ss.str());
}

/**
 * @brief converts plugin::udf::error_info to jogasaki::error::error_info.
 * @param udf_error the UDF error info to convert
 * @return the converted jogasaki error info
 */
std::shared_ptr<jogasaki::error::error_info> convert_udf_error(
    plugin::udf::error_info const& udf_error) {
    // Convert gRPC status code to jogasaki error_code
    // For now, map all UDF errors to evaluation_exception with the message from UDF
    jogasaki::error_code code = jogasaki::error_code::evaluation_exception;
    std::ostringstream ss;
    ss << "UDF error(" << plugin::udf::to_string_view(udf_error.code()) << "): " << udf_error.message();
    return create_error_info(code, ss.str(), status::err_expression_evaluation_failure);
}

} // namespace
using base_stream = ::jogasaki::data::any_sequence_stream;

udf_any_sequence_stream::udf_any_sequence_stream(
    std::unique_ptr<plugin::udf::generic_record_stream> udf_stream,
    std::vector<jogasaki::udf::data::udf_wire_kind> column_types)
    : udf_stream_(std::move(udf_stream)), column_types_(std::move(column_types)) {}

base_stream::status_type udf_any_sequence_stream::try_next(any_sequence& seq) {
    if (!udf_stream_) { return status_type::end_of_stream; }

    plugin::udf::generic_record_impl record;
    auto status = udf_stream_->try_next(record);

    switch (status) {
        case plugin::udf::generic_record_stream_status::ok:
            assert_with_exception(!record.error(), "inconsistent status with record error state");
            return convert_record_to_sequence(record, seq) ? status_type::ok : status_type::error;
        case plugin::udf::generic_record_stream_status::error:
            if (record.error()) { // this must be true, but just in case
                VLOG_LP(log_error) << "UDF stream error: code="
                    << plugin::udf::to_string_view(record.error()->code())
                    << ", message=" << record.error()->message();
                seq.error(convert_udf_error(*record.error()));
            } else {
                // should not happen though
                VLOG_LP(log_error) << "UDF stream error (no error details in record)";
            }
            return status_type::error;
        case plugin::udf::generic_record_stream_status::end_of_stream:
            return status_type::end_of_stream;
        case plugin::udf::generic_record_stream_status::not_ready: return status_type::not_ready;
    }
    return status_type::error;
}

base_stream::status_type udf_any_sequence_stream::next(
    any_sequence& seq, std::optional<std::chrono::milliseconds> timeout) {
    if (!udf_stream_) { return status_type::end_of_stream; }

    plugin::udf::generic_record_impl record;
    auto status = udf_stream_->next(record, timeout);

    switch (status) {
        case plugin::udf::generic_record_stream_status::ok:
            assert_with_exception(!record.error(), "inconsistent status with record error state");
            return convert_record_to_sequence(record, seq) ? status_type::ok : status_type::error;
        case plugin::udf::generic_record_stream_status::error:
            if (record.error()) { // this must be true, but just in case
                VLOG_LP(log_error) << "UDF stream error: code="
                    << plugin::udf::to_string_view(record.error()->code())
                    << ", message=" << record.error()->message();
                seq.error(convert_udf_error(*record.error()));
            } else {
                // should not happen though
                VLOG_LP(log_error) << "UDF stream error (no error details in record)";
            }
            return status_type::error;
        case plugin::udf::generic_record_stream_status::end_of_stream:
            return status_type::end_of_stream;
        case plugin::udf::generic_record_stream_status::not_ready: return status_type::not_ready;
    }
    return status_type::error;
}

void udf_any_sequence_stream::close() {
    if (udf_stream_) { udf_stream_->close(); }
}
bool udf_any_sequence_stream::convert_record_to_sequence(
    plugin::udf::generic_record const& record, any_sequence& seq) {
    auto cursor = record.cursor();
    if (!cursor) { return false; }

    std::vector<any> values;
    values.reserve(column_types_.size());

    using kind = jogasaki::udf::data::udf_wire_kind;

    for (auto const& col_type : column_types_) {
        switch (col_type) {
            case kind::boolean: emplace_nullable<bool>(values, cursor->fetch_bool()); break;
            case kind::int4: emplace_nullable<std::int32_t>(values, cursor->fetch_int4()); break;
            case kind::int8: emplace_nullable<std::int64_t>(values, cursor->fetch_int8()); break;
            case kind::float4: emplace_nullable<float>(values, cursor->fetch_float()); break;
            case kind::float8: emplace_nullable<double>(values, cursor->fetch_double()); break;

            case kind::character:
                emplace_nullable_with<accessor::text>(values, cursor->fetch_string(),
                    [](auto const& s) { return accessor::text{s}; });
                break;

            case kind::octet:
                emplace_nullable_with<accessor::binary>(values, cursor->fetch_string(),
                    [](auto const& s) { return accessor::binary{s}; });
                break;

            case kind::decimal: append_decimal(values, *cursor); break;
            case kind::date: append_date(values, *cursor); break;
            case kind::time_of_day: append_time_of_day(values, *cursor); break;
            case kind::time_point: append_time_point(values, *cursor); break;
            case kind::time_point_with_time_zone: append_time_point_with_time_zone(values, *cursor); break;
            case kind::blob: append_blob(values, *cursor); break;
            case kind::clob: append_clob(values, *cursor); break;

            default: fail_unsupported();
        }
    }

    seq = any_sequence(std::move(values));
    return true;
}

} // namespace jogasaki::udf::data
