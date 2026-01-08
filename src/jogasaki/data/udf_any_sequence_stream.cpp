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

any_sequence_stream::status_type udf_any_sequence_stream::try_next(any_sequence& seq) {
    if (! udf_stream_) {
        return status_type::end_of_stream;
    }

    plugin::udf::generic_record_impl record;
    auto status = udf_stream_->try_next(record);

    switch (status) {
        case plugin::udf::generic_record_stream_status::ok:
            if (! convert_record_to_sequence(record, seq)) {
                return status_type::error;
            }
            return status_type::ok;
        case plugin::udf::generic_record_stream_status::error:
            return status_type::error;
        case plugin::udf::generic_record_stream_status::end_of_stream:
            return status_type::end_of_stream;
        case plugin::udf::generic_record_stream_status::not_ready:
            return status_type::not_ready;
    }
    return status_type::error;
}

any_sequence_stream::status_type udf_any_sequence_stream::next(
    any_sequence& seq,
    std::optional<std::chrono::milliseconds> timeout
) {
    if (! udf_stream_) {
        return status_type::end_of_stream;
    }

    plugin::udf::generic_record_impl record;
    auto status = udf_stream_->next(record, timeout);

    switch (status) {
        case plugin::udf::generic_record_stream_status::ok:
            if (! convert_record_to_sequence(record, seq)) {
                return status_type::error;
            }
            return status_type::ok;
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
    plugin::udf::generic_record const& record,
    any_sequence& seq
) {
    auto cursor = record.cursor();
    if (! cursor) {
        return false;
    }

    std::vector<any> values;
    values.reserve(column_types_.size());

    for (auto const& col_type : column_types_) {
        using kind = meta::field_type_kind;
        switch (col_type.kind()) {
            case kind::boolean: {
                auto val = cursor->fetch_bool();
                if (val) {
                    values.emplace_back(*val);
                } else {
                    values.emplace_back();
                }
                break;
            }
            case kind::int4: {
                auto val = cursor->fetch_int4();
                if (val) {
                    values.emplace_back(*val);
                } else {
                    values.emplace_back();
                }
                break;
            }
            case kind::int8: {
                auto val = cursor->fetch_int8();
                if (val) {
                    values.emplace_back(*val);
                } else {
                    values.emplace_back();
                }
                break;
            }
            case kind::float4: {
                auto val = cursor->fetch_float();
                if (val) {
                    values.emplace_back(*val);
                } else {
                    values.emplace_back();
                }
                break;
            }
            case kind::float8: {
                auto val = cursor->fetch_double();
                if (val) {
                    values.emplace_back(*val);
                } else {
                    values.emplace_back();
                }
                break;
            }
            case kind::character: {
                auto val = cursor->fetch_string();
                if (val) {
                    values.emplace_back(accessor::text{*val});
                } else {
                    values.emplace_back();
                }
                break;
            }
            default:
                // unsupported type - treat as null
                values.emplace_back();
                break;
        }
    }

    seq = any_sequence(std::move(values));
    return true;
}

}  // namespace jogasaki::data
