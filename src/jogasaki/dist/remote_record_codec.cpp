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
#include "remote_record_codec.h"

#include <chrono>
#include <iterator>
#include <limits>
#include <stdexcept>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/serializer/value_input.h>
#include <jogasaki/serializer/value_writer.h>

namespace jogasaki::dist {
namespace {
class buffer_writer {
public:
    bool write(char const* data, std::size_t length) {
        bytes_.append(data, length);
        return true;
    }
    std::string release() { return std::move(bytes_); }

private:
    std::string bytes_{};
};
using k = meta::field_type_kind;
} // namespace
bool remote_output_supported(meta::record_meta const& meta) noexcept {
    for (std::size_t i = 0; i < meta.field_count(); ++i) {
        switch (meta.at(i).kind()) {
            case k::boolean:
                break;
            case k::int4:
                break;
            case k::int8:
                break;
            case k::float4:
                break;
            case k::float8:
                break;
            case k::decimal:
                break;
            case k::character:
                break;
            case k::octet:
                break;
            case k::date:
                break;
            case k::time_of_day:
                break;
            case k::time_point:
                break;
            default:
                return false;
        }
    }
    return true;
}
std::string encode_remote_record(accessor::record_ref record, meta::record_meta const& meta) {
    if (!remote_output_supported(meta)) {
        throw std::invalid_argument("unsupported remote output type");
    }
    buffer_writer buffer{};
    serializer::value_writer<buffer_writer> out{buffer};
    out.write_row_begin(meta.field_count());
    for (std::size_t i = 0; i < meta.field_count(); ++i) {
        if (meta.nullable(i) && record.is_null(meta.nullity_offset(i))) {
            out.write_null();
            continue;
        }
        auto offset = meta.value_offset(i);
        switch (meta.at(i).kind()) {
            case k::boolean:
                out.write_int(record.get_value<meta::field_type_traits<k::boolean>::runtime_type>(offset));
                break;
            case k::int4:
                out.write_int(record.get_value<meta::field_type_traits<k::int4>::runtime_type>(offset));
                break;
            case k::int8:
                out.write_int(record.get_value<meta::field_type_traits<k::int8>::runtime_type>(offset));
                break;
            case k::float4:
                out.write_float4(record.get_value<meta::field_type_traits<k::float4>::runtime_type>(offset));
                break;
            case k::float8:
                out.write_float8(record.get_value<meta::field_type_traits<k::float8>::runtime_type>(offset));
                break;
            case k::decimal:
                out.write_decimal(record.get_value<meta::field_type_traits<k::decimal>::runtime_type>(offset));
                break;
            case k::character: {
                auto value = record.get_value<meta::field_type_traits<k::character>::runtime_type>(offset);
                out.write_character(static_cast<std::string_view>(value));
                break;
            }
            case k::octet: {
                auto value = record.get_value<meta::field_type_traits<k::octet>::runtime_type>(offset);
                out.write_octet(static_cast<std::string_view>(value));
                break;
            }
            case k::date:
                out.write_date(record.get_value<meta::field_type_traits<k::date>::runtime_type>(offset));
                break;
            case k::time_of_day: {
                auto value = record.get_value<meta::field_type_traits<k::time_of_day>::runtime_type>(offset);
                if (meta.at(i).option_unsafe<k::time_of_day>()->with_offset_) {
                    auto zone = global::config_pool()->zone_offset();
                    value += std::chrono::minutes(zone);
                    out.write_time_of_day_with_offset(value, zone);
                } else {
                    out.write_time_of_day(value);
                }
                break;
            }
            case k::time_point: {
                auto value = record.get_value<meta::field_type_traits<k::time_point>::runtime_type>(offset);
                if (meta.at(i).option_unsafe<k::time_point>()->with_offset_) {
                    auto zone = global::config_pool()->zone_offset();
                    value += std::chrono::minutes(zone);
                    out.write_time_point_with_offset(value, zone);
                } else {
                    out.write_time_point(value);
                }
                break;
            }
            default:
                throw std::invalid_argument("unsupported remote output type");
        }
    }
    return buffer.release();
}
static void decode_remote_field(char const*& pos, char const* end, meta::record_meta const& meta,
                                accessor::record_ref record, std::size_t i) {
    auto offset = meta.value_offset(i);
    switch (meta.at(i).kind()) {
        case k::boolean: {
            auto v = serializer::read_int(pos, end);
            if (v != 0 && v != 1) {
                throw std::invalid_argument("invalid remote boolean");
            }
            record.set_value<std::int8_t>(offset, static_cast<std::int8_t>(v));
            break;
        }
        case k::int4: {
            auto v = serializer::read_int(pos, end);
            if (v < std::numeric_limits<std::int32_t>::min() || v > std::numeric_limits<std::int32_t>::max()) {
                throw std::invalid_argument("remote integer overflow");
            }
            record.set_value<std::int32_t>(offset, static_cast<std::int32_t>(v));
            break;
        }
        case k::int8:
            record.set_value<meta::field_type_traits<k::int8>::runtime_type>(offset, serializer::read_int(pos, end));
            break;
        case k::float4:
            record.set_value<meta::field_type_traits<k::float4>::runtime_type>(offset,
                                                                               serializer::read_float4(pos, end));
            break;
        case k::float8:
            record.set_value<meta::field_type_traits<k::float8>::runtime_type>(offset,
                                                                               serializer::read_float8(pos, end));
            break;
        case k::decimal:
            record.set_value<meta::field_type_traits<k::decimal>::runtime_type>(offset,
                                                                                serializer::read_decimal(pos, end));
            break;
        case k::date:
            record.set_value<meta::field_type_traits<k::date>::runtime_type>(offset, serializer::read_date(pos, end));
            break;
        case k::character:
            record.set_value<accessor::text>(offset, accessor::text{serializer::read_character(pos, end)});
            break;
        case k::octet:
            record.set_value<accessor::binary>(offset, accessor::binary{serializer::read_octet(pos, end)});
            break;
        case k::time_of_day: {
            auto value = meta.at(i).option_unsafe<k::time_of_day>()->with_offset_
                             ? [&] {
                                   auto [v, zone] = serializer::read_time_of_day_with_offset(pos, end);
                                   return v - std::chrono::minutes(zone);
                               }()
                             : serializer::read_time_of_day(pos, end);
            record.set_value<meta::field_type_traits<k::time_of_day>::runtime_type>(offset, value);
            break;
        }
        case k::time_point: {
            auto value = meta.at(i).option_unsafe<k::time_point>()->with_offset_
                             ? [&] {
                                   auto [v, zone] = serializer::read_time_point_with_offset(pos, end);
                                   return v - std::chrono::minutes(zone);
                               }()
                             : serializer::read_time_point(pos, end);
            record.set_value<meta::field_type_traits<k::time_point>::runtime_type>(offset, value);
            break;
        }
        default:
            throw std::invalid_argument("unsupported remote output type");
    }
}

bool decode_remote_record(std::string_view payload, meta::record_meta const& meta, accessor::record_ref record,
                          std::string& error) {
    auto* pos = payload.data();
    auto* end = payload.data();
    std::advance(end, payload.size());
    try {
        if (!remote_output_supported(meta) || serializer::read_row_begin(pos, end) != meta.field_count()) {
            throw std::invalid_argument("remote row schema mismatch");
        }
        for (std::size_t i = 0; i < meta.field_count(); ++i) {
            bool null = serializer::peek_type(pos, end) == serializer::entry_type::null;
            if (null && !meta.nullable(i)) {
                throw std::invalid_argument("NULL in a non-nullable remote column");
            }
            if (meta.nullable(i)) {
                record.set_null(meta.nullity_offset(i), null);
            }
            if (null) {
                serializer::read_null(pos, end);
                continue;
            }
            decode_remote_field(pos, end, meta, record, i);
        }
        if (pos != end) {
            throw std::invalid_argument("trailing remote row data");
        }
        return true;
    } catch (std::exception const& e) {
        error = e.what();
        return false;
    }
}
} // namespace jogasaki::dist
