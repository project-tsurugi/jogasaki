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
#include <utility>
#include <vector>

#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/udf_wire_codec.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/udf/generic_record.h>
#include <jogasaki/udf/generic_record_impl.h>

namespace jogasaki::data {

namespace {

template <class T, class Opt> void emplace_nullable(std::vector<any>& values, Opt const& opt) {
    if (opt) {
        values.emplace_back(std::in_place_type<T>, *opt);
    } else {
        values.emplace_back();
    }
}

template <class T, class Opt, class F>
void emplace_nullable_with(std::vector<any>& values, Opt const& opt, F const& f) {
    if (opt) {
        values.emplace_back(std::in_place_type<T>, f(*opt));
    } else {
        values.emplace_back();
    }
}

void append_decimal(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    auto unscaled_opt = cursor.fetch_string();
    auto exponent_opt = cursor.fetch_int4();
    if (unscaled_opt && exponent_opt) {
        auto triple = jogasaki::data::decode_decimal_triple(*unscaled_opt, *exponent_opt);
        values.emplace_back(std::in_place_type<runtime_t<meta::field_type_kind::decimal>>, triple);
    } else {
        values.emplace_back();
    }
}

void append_date(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    if (auto days_opt = cursor.fetch_int4()) {
        auto date = jogasaki::data::decode_date_from_wire(*days_opt);
        values.emplace_back(std::in_place_type<runtime_t<meta::field_type_kind::date>>, date);
    } else {
        values.emplace_back();
    }
}

void append_time_of_day(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    if (auto nanos_opt = cursor.fetch_int8()) {
        auto tod = jogasaki::data::decode_time_of_day_from_wire(*nanos_opt);
        values.emplace_back(std::in_place_type<runtime_t<meta::field_type_kind::time_of_day>>, tod);
    } else {
        values.emplace_back();
    }
}

void append_time_point(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    auto sec_opt  = cursor.fetch_int8();
    auto nano_opt = cursor.fetch_uint4();
    if (sec_opt && nano_opt) {
        auto tp = jogasaki::data::decode_time_point_from_wire(*sec_opt, *nano_opt);
        values.emplace_back(std::in_place_type<runtime_t<meta::field_type_kind::time_point>>, tp);
    } else {
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
        auto ref = decode_fn(*storage_opt, *object_opt, *tag_opt, prov_opt);
        values.emplace_back(std::in_place_type<Ref>, ref);
    } else {
        values.emplace_back();
    }
}

void append_blob(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    append_lob_reference<runtime_t<meta::field_type_kind::blob>>(values, cursor,
        [](std::uint64_t s, std::uint64_t o, std::uint64_t t, std::optional<bool> p) {
            return jogasaki::data::decode_blob_reference_from_wire(s, o, t, p);
        });
}

void append_clob(std::vector<any>& values, plugin::udf::generic_record_cursor& cursor) {
    append_lob_reference<runtime_t<meta::field_type_kind::clob>>(values, cursor,
        [](std::uint64_t s, std::uint64_t o, std::uint64_t t, std::optional<bool> p) {
            return jogasaki::data::decode_clob_reference_from_wire(s, o, t, p);
        });
}

void fail_unsupported(meta::field_type const& col_type) {
    std::ostringstream ss;
    ss << "unsupported meta::field_type in convert_record_to_sequence(): kind="
       << meta::to_string_view(col_type.kind()) << " field_type=" << col_type;
    fail_with_exception_msg(ss.str());
}

} // namespace

udf_any_sequence_stream::udf_any_sequence_stream(
    std::unique_ptr<plugin::udf::generic_record_stream> udf_stream,
    std::vector<meta::field_type> column_types)
    : udf_stream_(std::move(udf_stream)), column_types_(std::move(column_types)) {}

any_sequence_stream::status_type udf_any_sequence_stream::try_next(any_sequence& seq) {
    if (!udf_stream_) { return status_type::end_of_stream; }

    plugin::udf::generic_record_impl record;
    auto status = udf_stream_->try_next(record);

    switch (status) {
        case plugin::udf::generic_record_stream_status::ok:
            return convert_record_to_sequence(record, seq) ? status_type::ok : status_type::error;
        case plugin::udf::generic_record_stream_status::error: return status_type::error;
        case plugin::udf::generic_record_stream_status::end_of_stream:
            return status_type::end_of_stream;
        case plugin::udf::generic_record_stream_status::not_ready: return status_type::not_ready;
    }
    return status_type::error;
}

any_sequence_stream::status_type udf_any_sequence_stream::next(
    any_sequence& seq, std::optional<std::chrono::milliseconds> timeout) {
    if (!udf_stream_) { return status_type::end_of_stream; }

    plugin::udf::generic_record_impl record;
    auto status = udf_stream_->next(record, timeout);

    switch (status) {
        case plugin::udf::generic_record_stream_status::ok:
            return convert_record_to_sequence(record, seq) ? status_type::ok : status_type::error;
        case plugin::udf::generic_record_stream_status::error: return status_type::error;
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

    using kind = meta::field_type_kind;

    for (auto const& col_type : column_types_) {
        switch (col_type.kind()) {
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
            case kind::blob: append_blob(values, *cursor); break;
            case kind::clob: append_clob(values, *cursor); break;

            default: fail_unsupported(col_type);
        }
    }

    seq = any_sequence(std::move(values));
    return true;
}

} // namespace jogasaki::data
