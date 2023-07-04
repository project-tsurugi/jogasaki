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
#include <jogasaki/serializer/value_output.h>
#include <takatori/util/exception.h>

using buffer = takatori::util::buffer_view;
using takatori::util::throw_exception;

namespace jogasaki::api::kvsservice {

static std::size_t calc_max_bufsize(std::vector<tateyama::proto::kvs::data::Value const*> &values) {
    std::size_t len = 0;
    for (auto value: values) {
        len += 1; // for type data
        switch (value->value_case()) {
            case tateyama::proto::kvs::data::Value::ValueCase::kBooleanValue:
                len += 4;
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kInt4Value:
                len += 4;
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kInt8Value:
                len += 8;
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kFloat4Value:
                len += 4;
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kFloat8Value:
                len += 8;
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kCharacterValue:
                // string length (8 bytes) + string data
                len += 8 + value->character_value().size();
                break;
            default:
                takatori::util::throw_exception(std::logic_error{"not implemented: unknown value_case"});
                break;
        }
    }
    len = ((len + 63) / 64) * 64;
    return len;
}

status serialize(std::vector<tateyama::proto::kvs::data::Value const*> &values, std::string &results) {
    results.resize(calc_max_bufsize(values));
    buffer buf { results.data(), results.size() };
    buffer::iterator iter = buf.begin();
    for (auto value : values) {
        bool b = false;
        switch (value->value_case()) {
            case tateyama::proto::kvs::data::Value::ValueCase::kBooleanValue:
                b = jogasaki::serializer::write_int(value->boolean_value() ? 1 : 0, iter, buf.end());
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kInt4Value:
                b = jogasaki::serializer::write_int(value->int4_value(), iter, buf.end());
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kInt8Value:
                b = jogasaki::serializer::write_int(value->int8_value(), iter, buf.end());
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kFloat4Value:
                b = jogasaki::serializer::write_float4(value->float4_value(), iter, buf.end());
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kFloat8Value:
                b = jogasaki::serializer::write_float8(value->float8_value(), iter, buf.end());
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kCharacterValue:
                b = jogasaki::serializer::write_character(value->character_value(), iter, buf.end());
                break;
                /*
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
        if (!b) {
            throw std::runtime_error("underflow");
        }
    }
    // TODO necessary?
//    if (!jogasaki::serializer::write_end_of_contents(iter, buf.end())) {
//        takatori::util::throw_exception(std::logic_error{"write_end_of_contents failed"});
//    }
    results.resize(std::distance(buf.begin(), iter));
    return status::ok;
}

void deserialize(takatori::type::data const &data, takatori::util::const_buffer_view &view,
                   const char *iter, tateyama::proto::kvs::data::Value *value) {
    switch (data.kind()) {
        case takatori::type::type_kind::int4:
            value->set_int4_value(jogasaki::serializer::read_int(iter, view.end()));
            break;
        case takatori::type::type_kind::int8:
            value->set_int8_value(jogasaki::serializer::read_int(iter, view.end()));
            break;
        case takatori::type::type_kind::float4:
            value->set_float4_value(jogasaki::serializer::read_float4(iter, view.end()));
            break;
        case takatori::type::type_kind::float8:
            value->set_float8_value(jogasaki::serializer::read_float8(iter, view.end()));
            break;
        case takatori::type::type_kind::character: {
            auto s = jogasaki::serializer::read_character(iter, view.end());
            value->set_character_value(s.data(), s.size());
            break;
        }
        default:
            takatori::util::throw_exception(std::logic_error{"not implemented: unknown value_case"});
            break;
    }
}

}
