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
#include <jogasaki/udf/log/logging_prefix.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/convert_offset.h>

namespace jogasaki::udf::data {

namespace {
using any = ::jogasaki::data::any;

template <class T, class Opt> void emplace_nullable(std::vector<any>& values, Opt const& opt) {
    if (opt) {
        values.emplace_back(std::in_place_type<T>, *opt);
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << typeid(T).name() << ":" << *opt;
    } else {
        values.emplace_back();
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << typeid(T).name() << ":NULL";
    }
}

template <class T, class Opt, class F>
void emplace_nullable_with(std::vector<any>& values, Opt const& opt, F const& f) {
    if (opt) {
        values.emplace_back(std::in_place_type<T>, f(*opt));
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << typeid(T).name() << ":" << *opt;
    } else {
        values.emplace_back();
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << typeid(T).name() << ":NULL";
    }
}

void append_decimal(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    if (auto v = cursor.fetch_decimal()) {
        if (VLOG_IS_ON(log_trace)) {
            std::string_view bin_view{v->unscaled_value.data(), v->unscaled_value.size()};
            VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << "decimal:("
                               << utils::binary_printer{bin_view}.show_hyphen(false) << ","
                               << v->exponent << ")";
        }
        auto triple = jogasaki::udf::data::decode_decimal_triple(v->unscaled_value, v->exponent);
        values.emplace_back(std::in_place_type<runtime_t<meta::field_type_kind::decimal>>, triple);
    } else {
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << "decimal:NULL";
        values.emplace_back();
    }
}

void append_date(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    if (auto v = cursor.fetch_date()) {
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << "date:" << v->days;
        auto date = jogasaki::udf::data::decode_date_from_wire(v->days);
        values.emplace_back(std::in_place_type<runtime_t<meta::field_type_kind::date>>, date);
    } else {
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << "date:NULL";
        values.emplace_back();
    }
}

void append_time_of_day(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    if (auto v = cursor.fetch_local_time()) {
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << "time_of_day:" << v->nanos;
        auto tod = jogasaki::udf::data::decode_time_of_day_from_wire(v->nanos);
        values.emplace_back(std::in_place_type<runtime_t<meta::field_type_kind::time_of_day>>, tod);
    } else {
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << "time_of_day:NULL";
        values.emplace_back();
    }
}

void append_time_point(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    if (auto v = cursor.fetch_local_datetime()) {
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << "time_point:("
                           << v->offset_seconds << "," << v->nano_adjustment << ")";
        auto tp =
            jogasaki::udf::data::decode_time_point_from_wire(v->offset_seconds, v->nano_adjustment);
        values.emplace_back(std::in_place_type<runtime_t<meta::field_type_kind::time_point>>, tp);
    } else {
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << "time_point:NULL";
        values.emplace_back();
    }
}

void append_time_point_with_time_zone(
    std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    if (auto v = cursor.fetch_offset_datetime()) {
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << "time_point_tz:("
                           << v->offset_seconds << "," << v->nano_adjustment << ","
                           << v->time_zone_offset << ")";
        auto tp_local =
            jogasaki::udf::data::decode_time_point_from_wire(v->offset_seconds, v->nano_adjustment);
        values.emplace_back(
            std::in_place_type<runtime_t<meta::field_type_kind::time_point>>, tp_local);
    } else {
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << "time_point_tz:NULL";
        values.emplace_back();
    }
}

template <class Ref, class FetchFn>
void append_lob_reference(std::vector<any>& values, FetchFn const& fetch_fn) {
    auto value_opt = fetch_fn();

    if (!value_opt) {
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << "lob:NULL";
        values.emplace_back();
        return;
    }

    auto const& value = *value_opt;
    if (VLOG_IS_ON(log_trace)) {
        VLOG_LP(log_trace) << jogasaki::udf::log::udf_out_prefix << "lob:(" << value.storage_id
                           << "," << value.object_id << "," << value.tag << ","
                           << (value.provisioned ? "true" : "false") << ")";
    }

    if (value.storage_id == 1ULL) {
        if (value.provisioned) {
            values.emplace_back(std::in_place_type<Ref>,
                Ref{value.object_id, jogasaki::lob::lob_data_provider::datastore}.reference_tag(
                    value.tag));
        } else {
            values.emplace_back(
                std::in_place_type<Ref>, Ref{value.object_id}.reference_tag(value.tag));
        }
        return;
    }

    if (value.storage_id == 0ULL) {
        values.emplace_back(std::in_place_type<Ref>,
            Ref{value.object_id, jogasaki::lob::lob_data_provider::relay_service_session}
                .reference_tag(value.tag));
        return;
    }

    values.emplace_back();
}

void append_blob(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    append_lob_reference<runtime_t<meta::field_type_kind::blob>>(
        values, [&] { return cursor.fetch_blob_reference(); });
}

void append_clob(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    append_lob_reference<runtime_t<meta::field_type_kind::clob>>(
        values, [&] { return cursor.fetch_clob_reference(); });
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
    ss << "UDF error(" << plugin::udf::to_string_view(udf_error.code())
       << "): " << udf_error.message();
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
            assert_with_exception(!record.error());
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
            assert_with_exception(!record.error());
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
            case kind::time_point_with_time_zone:
                append_time_point_with_time_zone(values, *cursor);
                break;
            case kind::blob: append_blob(values, *cursor); break;
            case kind::clob: append_clob(values, *cursor); break;

            default: fail_unsupported();
        }
    }

    seq = any_sequence(std::move(values));
    return true;
}

} // namespace jogasaki::udf::data
