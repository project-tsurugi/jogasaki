/*
 * Copyright 2018-2023 tsurugi project.
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

#include <array>

#include <jogasaki/utils/decimal.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <takatori/type/decimal.h>
#include <takatori/util/exception.h>
#include "serializer.h"

using buffer = takatori::util::buffer_view;
using takatori::util::throw_exception;
using kind = jogasaki::meta::field_type_kind;

using takatori::util::throw_exception;

namespace jogasaki::api::kvsservice {

std::size_t get_bufsize(jogasaki::kvs::coding_spec const &spec, bool nullable, std::vector<column_data> const &list) {
    // NOTE empty_stream is created just for getting enough buffer size
    jogasaki::kvs::writable_stream empty_stream{nullptr, 0, true};
    auto s = serialize(spec, nullable, list, empty_stream);
    if (s != status::ok) {
        throw_exception(std::logic_error{"serialize with empty buffer failed"});
    }
    return empty_stream.size();
}

status serialize(jogasaki::kvs::coding_spec const &spec, bool nullable, std::vector<column_data> const &list,
                 jogasaki::kvs::writable_stream &results) {
    for (auto cd : list) {
        const auto value = cd.value();
        jogasaki::status s{};
        switch (value->value_case()) {
            case tateyama::proto::kvs::data::Value::ValueCase::kInt4Value: {
                data::any data{std::in_place_type<std::int32_t>, value->int4_value()};
                auto type = meta::field_type{meta::field_enum_tag<kind::int4>};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, type, spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, type, spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kInt8Value: {
                data::any data{std::in_place_type<std::int64_t>, value->int8_value()};
                auto type = meta::field_type{meta::field_enum_tag<kind::int8>};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, type, spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, type, spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kFloat4Value: {
                data::any data{std::in_place_type<std::float_t>, value->float4_value()};
                auto type = meta::field_type{meta::field_enum_tag<kind::float4>};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, type, spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, type, spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kFloat8Value: {
                data::any data{std::in_place_type<std::double_t>, value->float8_value()};
                auto type = meta::field_type{meta::field_enum_tag<kind::float8>};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, type, spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, type, spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kCharacterValue: {
                std::string_view view = value->character_value();
                accessor::text txt {view.data(), view.size()};
                data::any data{std::in_place_type<accessor::text>, txt};
                auto type = meta::field_type{meta::field_enum_tag<kind::character>};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, type, spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, type, spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kBooleanValue: {
                std::int8_t v = value->boolean_value() ? 1 : 0;
                data::any data{std::in_place_type<std::int8_t>, v};
                auto type = meta::field_type{meta::field_enum_tag<kind::boolean>};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, type, spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, type, spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kDecimalValue: {
                const auto &dec = value->decimal_value();
                auto triple = jogasaki::utils::read_decimal(dec.unscaled_value(), -dec.exponent());
                auto ctype = cd.column()->optional_type();
                if (ctype.empty()) {
                    return status::err_invalid_argument;
                }
                auto decimal_type = dynamic_cast<const takatori::type::decimal *>(ctype.get());
                auto type{meta::field_type{std::make_shared<meta::decimal_field_option>(
                        decimal_type->precision(), decimal_type->scale())}};
                data::any data{std::in_place_type<takatori::decimal::triple>, triple};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, type, spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, type, spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kDateValue: {
                takatori::datetime::date date{value->date_value()};
                data::any data{std::in_place_type<takatori::datetime::date>, date};
                auto type = meta::field_type{meta::field_enum_tag<kind::date>};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, type, spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, type, spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kTimeOfDayValue: {
                takatori::datetime::time_of_day::time_unit nanosec {value->time_of_day_value()};
                takatori::datetime::time_of_day time{nanosec};
                data::any data{std::in_place_type<takatori::datetime::time_of_day>, time};
                auto type = meta::field_type{std::make_shared<meta::time_of_day_field_option>()};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, type, spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, type, spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kTimePointValue: {
                 auto &tp = value->time_point_value();
                takatori::datetime::time_point::offset_type offset{tp.offset_seconds()};
                takatori::datetime::time_point::difference_type diff{tp.nano_adjustment()};
                takatori::datetime::time_point time{offset, diff};
                data::any data{std::in_place_type<takatori::datetime::time_point>, time};
                auto type = meta::field_type{std::make_shared<meta::time_point_field_option>()};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, type, spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, type, spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kTimeOfDayWithTimeZoneValue:
            case tateyama::proto::kvs::data::Value::ValueCase::kTimePointWithTimeZoneValue:
            case tateyama::proto::kvs::data::Value::ValueCase::kDatetimeIntervalValue:
            default:
                takatori::util::throw_exception(std::logic_error{"not implemented: unknown value_case"});
                break;
        }
        if (s != jogasaki::status::ok) {
            return status::err_invalid_argument;
        }
    }
    return status::ok;
}

status deserialize(jogasaki::kvs::coding_spec const &spec, bool nullable, yugawara::storage::column const &column,
                   jogasaki::kvs::readable_stream &stream, tateyama::proto::kvs::data::Value *value) {
    data::any dest{};
    jogasaki::status s{};
    switch (column.type().kind()) {
        case takatori::type::type_kind::int4: {
            auto type = meta::field_type{meta::field_enum_tag<kind::int4>};
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, type, spec, dest);
            } else {
                s = jogasaki::kvs::decode(stream, type, spec, dest);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument;
            }
            value->set_int4_value(dest.to<std::int32_t>());
            break;
        }
        case takatori::type::type_kind::int8: {
            auto type = meta::field_type{meta::field_enum_tag<kind::int8>};
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, type, spec, dest);
            } else {
                s = jogasaki::kvs::decode(stream, type, spec, dest);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument;
            }
            value->set_int8_value(dest.to<std::int64_t>());
            break;
        }
        case takatori::type::type_kind::float4: {
            auto type = meta::field_type{meta::field_enum_tag<kind::float4>};
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, type, spec, dest);
            } else {
                s = jogasaki::kvs::decode(stream, type, spec, dest);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument;
            }
            value->set_float4_value(dest.to<std::float_t>());
            break;
        }
        case takatori::type::type_kind::float8: {
            auto type = meta::field_type{meta::field_enum_tag<kind::float8>};
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, type, spec, dest);
            } else {
                s = jogasaki::kvs::decode(stream, type, spec, dest);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument;
            }
            value->set_float8_value(dest.to<std::double_t>());
            break;
        }
        case takatori::type::type_kind::character: {
            auto type = meta::field_type{meta::field_enum_tag<kind::character>};
            jogasaki::memory::page_pool pool{};
            jogasaki::memory::lifo_paged_memory_resource mem{&pool};
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, type, spec, dest, &mem);
            } else {
                s = jogasaki::kvs::decode(stream, type, spec, dest, &mem);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument;
            }
            auto txt = dest.to<accessor::text>();
            auto sv = static_cast<std::string_view>(txt);
            value->set_character_value(sv.data(), sv.size());
            break;
        }
        case takatori::type::type_kind::boolean: {
            auto type = meta::field_type{meta::field_enum_tag<kind::boolean>};
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, type, spec, dest);
            } else {
                s = jogasaki::kvs::decode(stream, type, spec, dest);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument;
            }
            value->set_boolean_value(dest.to<bool>());
            break;
        }
        case takatori::type::type_kind::decimal: {
            auto decimal_type = dynamic_cast<const takatori::type::decimal *>(&column.type());
            auto type{meta::field_type{std::make_shared<meta::decimal_field_option>(
                    decimal_type->precision(), decimal_type->scale())}};
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, type, spec, dest);
            } else {
                s = jogasaki::kvs::decode(stream, type, spec, dest);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument;
            }
            auto triple = dest.to<takatori::decimal::triple>();
            const auto lo = triple.coefficient_low();
            const auto hi = triple.coefficient_high();
            std::string buf{};
            buf.reserve(sizeof(lo) + sizeof(hi));
            auto v = lo;
            for (int i = 0; i < 8; i++) {
                buf[15 - i] = static_cast<char>(v & 0xffU);
                v >>= 8U;
            }
            v = hi;
            for (int i = 0; i < 8; i++) {
                buf[7 - i] = static_cast<char>(v & 0xffU);
                v >>= 8U;
            }
            auto *decimal = new tateyama::proto::kvs::data::Decimal();
            decimal->set_unscaled_value(buf.data(), buf.size());
            decimal->set_exponent(triple.exponent());
            value->set_allocated_decimal_value(decimal);
            break;
        }
        case takatori::type::type_kind::date: {
            auto type = meta::field_type{meta::field_enum_tag<kind::date>};
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, type, spec, dest);
            } else {
                s = jogasaki::kvs::decode(stream, type, spec, dest);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument;
            }
            auto date = dest.to<takatori::datetime::date>();
            value->set_date_value(date.days_since_epoch());
            break;
        }
        case takatori::type::type_kind::time_of_day: {
            auto type = meta::field_type{std::make_shared<meta::time_of_day_field_option>()};
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, type, spec, dest);
            } else {
                s = jogasaki::kvs::decode(stream, type, spec, dest);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument;
            }
            auto time = dest.to<takatori::datetime::time_of_day>();
            value->set_time_of_day_value(time.time_since_epoch().count());
            break;
        }
        case takatori::type::type_kind::time_point: {
            auto type = meta::field_type{std::make_shared<meta::time_point_field_option>()};
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, type, spec, dest);
            } else {
                s = jogasaki::kvs::decode(stream, type, spec, dest);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument;
            }
            auto tp = dest.to<takatori::datetime::time_point>();
            auto timepoint = new tateyama::proto::kvs::data::TimePoint();
            timepoint->set_offset_seconds(tp.seconds_since_epoch().count());
            timepoint->set_nano_adjustment(tp.subsecond().count());
            value->set_allocated_time_point_value(timepoint);
            break;
        }
        default:
            takatori::util::throw_exception(std::logic_error{"not implemented: unknown value_case"});
            break;
    }
    return status::ok;
}

}
