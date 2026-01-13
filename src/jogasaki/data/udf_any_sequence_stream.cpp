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
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/udf/generic_record.h>
#include <jogasaki/udf/generic_record_impl.h>

namespace jogasaki::data {

udf_any_sequence_stream::udf_any_sequence_stream(
    std::unique_ptr<plugin::udf::generic_record_stream> udf_stream,
    std::vector<meta::field_type> column_types
) :
    udf_stream_(std::move(udf_stream)),
    column_types_(std::move(column_types))
{}

any_sequence_stream::status_type
udf_any_sequence_stream::try_next(any_sequence& seq) {
    if (!udf_stream_) {
        return status_type::end_of_stream;
    }

    plugin::udf::generic_record_impl record;
    auto status = udf_stream_->try_next(record);

    switch (status) {
        case plugin::udf::generic_record_stream_status::ok:
            return convert_record_to_sequence(record, seq)
                ? status_type::ok
                : status_type::error;
        case plugin::udf::generic_record_stream_status::error:
            return status_type::error;
        case plugin::udf::generic_record_stream_status::end_of_stream:
            return status_type::end_of_stream;
        case plugin::udf::generic_record_stream_status::not_ready:
            return status_type::not_ready;
    }
    return status_type::error;
}

any_sequence_stream::status_type
udf_any_sequence_stream::next(
    any_sequence& seq,
    std::optional<std::chrono::milliseconds> timeout
) {
    if (!udf_stream_) {
        return status_type::end_of_stream;
    }

    plugin::udf::generic_record_impl record;
    auto status = udf_stream_->next(record, timeout);

    switch (status) {
        case plugin::udf::generic_record_stream_status::ok:
            return convert_record_to_sequence(record, seq)
                ? status_type::ok
                : status_type::error;
        case plugin::udf::generic_record_stream_status::error:
            return status_type::error;
        case plugin::udf::generic_record_stream_status::end_of_stream:
            return status_type::end_of_stream;
        case plugin::udf::generic_record_stream_status::not_ready:
            return status_type::not_ready;
    }
    return status_type::error;
}

void udf_any_sequence_stream::close() {
    if (udf_stream_) {
        udf_stream_->close();
    }
}

bool udf_any_sequence_stream::convert_record_to_sequence(
    plugin::udf::generic_record const& record, any_sequence& seq) {
    auto cursor = record.cursor();
    if (!cursor) { return false; }

    std::vector<any> values;
    values.reserve(column_types_.size());

    auto emplace_nullable = [&values]<class T, class Opt>(Opt const& opt) {
        if (opt) {
            values.emplace_back(std::in_place_type<T>, *opt);
        } else {
            values.emplace_back();
        }
    };

    auto emplace_nullable_with = [&values]<class T, class Opt, class F>(
                                     Opt const& opt, F const& f) {
        if (opt) {
            values.emplace_back(std::in_place_type<T>, f(*opt));
        } else {
            values.emplace_back();
        }
    };

    for (auto const& col_type : column_types_) {
        using kind = meta::field_type_kind;

        switch (col_type.kind()) {
            case kind::boolean:
                emplace_nullable.template operator()<bool>(cursor->fetch_bool());
                break;

            case kind::int4:
                emplace_nullable.template operator()<std::int32_t>(cursor->fetch_int4());
                break;

            case kind::int8:
                emplace_nullable.template operator()<std::int64_t>(cursor->fetch_int8());
                break;

            case kind::float4:
                emplace_nullable.template operator()<float>(cursor->fetch_float());
                break;

            case kind::float8:
                emplace_nullable.template operator()<double>(cursor->fetch_double());
                break;

            case kind::character:
                emplace_nullable_with.template operator()<accessor::text>(
                    cursor->fetch_string(), [](auto const& s) { return accessor::text{s}; });
                break;
            case kind::octet:
                emplace_nullable_with.template operator()<accessor::binary>(
                    cursor->fetch_string(), [](auto const& s) { return accessor::binary{s}; });
                break;
            case kind::decimal:
            case kind::date:
            case kind::time_of_day:
            case kind::time_point:
            case kind::time_interval:
            case kind::blob:
            case kind::clob:
                values.emplace_back();
                break;
            default: {
                std::ostringstream ss;
                ss << "unsupported meta::field_type in convert_record_to_sequence(): kind="
                   << meta::to_string_view(col_type.kind());
                fail_with_exception_msg(ss.str());
            }
        }
    }

    seq = any_sequence(std::move(values));
    return true;
}

} // namespace jogasaki::data
