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

#include "serializer.h"
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <takatori/util/exception.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/aligned_buffer.h>

using buffer = takatori::util::buffer_view;
using takatori::util::throw_exception;
using kind = jogasaki::meta::field_type_kind;

namespace jogasaki::api::kvsservice {

status serialize(jogasaki::kvs::coding_spec const &spec, bool nullable, std::vector<tateyama::proto::kvs::data::Value const*> &values,
                 jogasaki::kvs::writable_stream &results) {
    for (auto value: values) {
        jogasaki::status s{};
        switch (value->value_case()) {
            case tateyama::proto::kvs::data::Value::ValueCase::kInt4Value: {
                data::any data{std::in_place_type<std::int32_t>, value->int4_value()};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, meta::field_type{meta::field_enum_tag<kind::int4>},
                                                       spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, meta::field_type{meta::field_enum_tag<kind::int4>},
                                              spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kInt8Value: {
                data::any data{std::in_place_type<std::int64_t>, value->int8_value()};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, meta::field_type{meta::field_enum_tag<kind::int8>},
                                                       spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, meta::field_type{meta::field_enum_tag<kind::int8>},
                                              spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kFloat4Value: {
                data::any data{std::in_place_type<std::float_t>, value->float4_value()};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, meta::field_type{meta::field_enum_tag<kind::float4>},
                                                       spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, meta::field_type{meta::field_enum_tag<kind::float4>},
                                              spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kFloat8Value: {
                data::any data{std::in_place_type<std::double_t>, value->float8_value()};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, meta::field_type{meta::field_enum_tag<kind::float8>},
                                                       spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, meta::field_type{meta::field_enum_tag<kind::float8>},
                                              spec, results);
                }
                break;
            }
            case tateyama::proto::kvs::data::Value::ValueCase::kCharacterValue: {
                std::string_view view = value->character_value();
                accessor::text txt {view.data(), view.size()};
                data::any data{std::in_place_type<accessor::text>, txt};
                if (nullable) {
                    s = jogasaki::kvs::encode_nullable(data, meta::field_type{meta::field_enum_tag<kind::character>},
                                                       spec, results);
                } else {
                    s = jogasaki::kvs::encode(data, meta::field_type{meta::field_enum_tag<kind::character>},
                                              spec, results);
                }
                break;
            }
                /*
            case tateyama::proto::kvs::data::Value::ValueCase::kBooleanValue:
            case tateyama::proto::kvs::data::Value::ValueCase::kDecimalValue:
            case tateyama::proto::kvs::data::Value::ValueCase::kOctetValue:
            case tateyama::proto::kvs::data::Value::ValueCase::kDateValue:
            case tateyama::proto::kvs::data::Value::ValueCase::kTimeOfDayValue:
            case tateyama::proto::kvs::data::Value::ValueCase::kTimePointValue:
            case tateyama::proto::kvs::data::Value::ValueCase::kDatetimeIntervalValue:
            case tateyama::proto::kvs::data::Value::ValueCase::kTimeOfDayWithTimeZoneValue:
            case tateyama::proto::kvs::data::Value::ValueCase::kTimePointWithTimeZoneValue:
                 */
            default:
                takatori::util::throw_exception(std::logic_error{"not implemented: unknown value_case"});
                break;
        }
        if (s != jogasaki::status::ok) {
            return status::err_invalid_argument; // FIXME
        }
    }
    return status::ok;
}

status deserialize(jogasaki::kvs::coding_spec const &spec, bool nullable, takatori::type::data const &data, jogasaki::kvs::readable_stream &stream, tateyama::proto::kvs::data::Value *value) {
    data::any dest{};
    jogasaki::status s{};
    switch (data.kind()) {
        case takatori::type::type_kind::int4:
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, meta::field_type{meta::field_enum_tag<kind::int4>},
                                                   spec, dest);
            } else {
                s = jogasaki::kvs::decode(stream, meta::field_type{meta::field_enum_tag<kind::int4>},
                                                   spec, dest);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument; // FIXME
            }
            value->set_int4_value(dest.to<std::int32_t>());
            break;
        case takatori::type::type_kind::int8:
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, meta::field_type{meta::field_enum_tag<kind::int8>},
                                                   spec, dest);
            } else {
                s = jogasaki::kvs::decode(stream, meta::field_type{meta::field_enum_tag<kind::int8>},
                                          spec, dest);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument; // FIXME
            }
            value->set_int8_value(dest.to<std::int64_t>());
            break;
        case takatori::type::type_kind::float4:
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, meta::field_type{meta::field_enum_tag<kind::float4>},
                                                   spec, dest);
            } else {
                s = jogasaki::kvs::decode(stream, meta::field_type{meta::field_enum_tag<kind::float4>},
                                          spec, dest);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument; // FIXME
            }
            value->set_float4_value(dest.to<std::float_t>());
            break;
        case takatori::type::type_kind::float8:
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, meta::field_type{meta::field_enum_tag<kind::float8>},
                                                   spec, dest);
            } else {
                s = jogasaki::kvs::decode(stream, meta::field_type{meta::field_enum_tag<kind::float8>},
                                          spec, dest);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument; // FIXME
            }
            value->set_float8_value(dest.to<std::double_t>());
            break;
        case takatori::type::type_kind::character: {
            jogasaki::memory::page_pool pool{};
            jogasaki::memory::monotonic_paged_memory_resource mem{&pool}; // TODO correct?
            if (nullable) {
                s = jogasaki::kvs::decode_nullable(stream, meta::field_type{meta::field_enum_tag<kind::character>},
                                                   spec, dest, &mem);
            } else {
                s = jogasaki::kvs::decode(stream, meta::field_type{meta::field_enum_tag<kind::character>},
                                          spec, dest, &mem);
            }
            if (s != jogasaki::status::ok) {
                return status::err_invalid_argument; // FIXME
            }
            auto txt = dest.to<accessor::text>();
            auto sv = static_cast<std::string_view>(txt);
            value->set_character_value(sv.data(), sv.size());
            break;
        }
        default:
            takatori::util::throw_exception(std::logic_error{"not implemented: unknown value_case"});
            break;
    }
    return status::ok;
}

}
