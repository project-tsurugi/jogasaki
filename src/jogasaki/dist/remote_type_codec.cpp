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
#include "remote_type_codec.h"

#include <iterator>
#include <limits>
#include <stdexcept>

#include <takatori/type/character.h>
#include <takatori/type/date.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/util/downcast.h>
#include <takatori/value/character.h>
#include <takatori/value/date.h>
#include <takatori/value/decimal.h>
#include <takatori/value/octet.h>
#include <takatori/value/primitive.h>
#include <takatori/value/time_of_day.h>
#include <takatori/value/time_point.h>

#include <jogasaki/proto/distributed_query/remote_execution_block.pb.h>
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
} // namespace
std::string encode_remote_type(takatori::type::data const& data) {
    using namespace takatori;
    proto::distributed_query::PortableType out{};
    out.set_kind(data.kind() == type::type_kind::boolean ? "boolean" : std::string{type::to_string_view(data.kind())});
    switch (data.kind()) {
        case type::type_kind::boolean:
            break;
        case type::type_kind::int4:
            break;
        case type::type_kind::int8:
            break;
        case type::type_kind::float4:
            break;
        case type::type_kind::float8:
            break;
        case type::type_kind::date:
            break;
        case type::type_kind::unknown:
            break;
        case type::type_kind::decimal: {
            auto const& t = takatori::util::unsafe_downcast<type::decimal>(data);
            if (t.precision()) {
                out.set_precision(*t.precision());
            }
            if (t.scale()) {
                out.set_scale(*t.scale());
            }
            break;
        }
        case type::type_kind::character: {
            auto const& t = takatori::util::unsafe_downcast<type::character>(data);
            out.set_varying(t.varying());
            if (t.length()) {
                out.set_length(*t.length());
            }
            break;
        }
        case type::type_kind::octet: {
            auto const& t = takatori::util::unsafe_downcast<type::octet>(data);
            out.set_varying(t.varying());
            if (t.length()) {
                out.set_length(*t.length());
            }
            break;
        }

        case type::type_kind::time_of_day:
            out.set_with_time_zone(takatori::util::unsafe_downcast<type::time_of_day>(data).with_time_zone());
            break;
        case type::type_kind::time_point:
            out.set_with_time_zone(takatori::util::unsafe_downcast<type::time_point>(data).with_time_zone());
            break;
        default:
            throw std::invalid_argument("unsupported remote execution type");
    }
    return out.SerializeAsString();
}
static bool valid_remote_type_options(proto::distributed_query::PortableType const& in) {
    bool const has_precision = in.precision_presence_case() == proto::distributed_query::PortableType::kPrecision;
    bool const has_scale = in.scale_presence_case() == proto::distributed_query::PortableType::kScale;
    bool const has_length = in.length_presence_case() == proto::distributed_query::PortableType::kLength;
    auto const& k = in.kind();
    if ((has_precision || has_scale) && k != "decimal") {
        return false;
    }
    if ((has_length || in.varying()) && k != "character" && k != "octet") {
        return false;
    }
    if (in.with_time_zone() && k != "time_of_day" && k != "time_point") {
        return false;
    }
    if (has_precision && in.precision() == 0) {
        return false;
    }
    if (has_precision && has_scale && in.scale() > in.precision()) {
        return false;
    }
    return true;
}

std::shared_ptr<takatori::type::data const> decode_remote_type(std::string_view payload) {
    using namespace takatori;
    proto::distributed_query::PortableType in{};
    if (payload.size() > 4096 || !in.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
        return {};
    }
    if (!valid_remote_type_options(in)) {
        return {};
    }
    bool const has_precision = in.precision_presence_case() == proto::distributed_query::PortableType::kPrecision;
    bool const has_scale = in.scale_presence_case() == proto::distributed_query::PortableType::kScale;
    bool const has_length = in.length_presence_case() == proto::distributed_query::PortableType::kLength;
    auto const& k = in.kind();
    std::optional<std::size_t> precision{};
    std::optional<std::size_t> scale{};
    std::optional<std::size_t> length{};
    if (has_precision) {
        precision = in.precision();
    }
    if (has_scale) {
        scale = in.scale();
    }
    if (has_length) {
        if (in.length() > std::numeric_limits<std::size_t>::max()) {
            return {};
        }
        length = in.length();
    }
    if (k == "boolean") {
        return std::make_shared<type::boolean>();
    }
    if (k == "int4") {
        return std::make_shared<type::int4>();
    }
    if (k == "int8") {
        return std::make_shared<type::int8>();
    }
    if (k == "float4") {
        return std::make_shared<type::float4>();
    }
    if (k == "float8") {
        return std::make_shared<type::float8>();
    }
    if (k == "date") {
        return std::make_shared<type::date>();
    }
    if (k == "unknown") {
        return std::make_shared<type::unknown>();
    }
    if (k == "decimal") {
        return std::make_shared<type::decimal>(precision, scale);
    }
    if (k == "character") {
        return std::make_shared<type::character>(type::varying_t{in.varying()}, length);
    }
    if (k == "octet") {
        return std::make_shared<type::octet>(type::varying_t{in.varying()}, length);
    }
    if (k == "time_of_day") {
        return std::make_shared<type::time_of_day>(type::with_time_zone_t{in.with_time_zone()});
    }
    if (k == "time_point") {
        return std::make_shared<type::time_point>(type::with_time_zone_t{in.with_time_zone()});
    }
    return {};
}
std::string encode_remote_literal(takatori::value::data const& data) {
    namespace value = takatori::value;
    buffer_writer buffer{};
    serializer::value_writer<buffer_writer> out{buffer};
    switch (data.kind()) {
        case value::value_kind::boolean:
            out.write_int((takatori::util::unsafe_downcast<value::boolean>(data).get() ? 1 : 0));
            break;
        case value::value_kind::int4:
            out.write_int(takatori::util::unsafe_downcast<value::int4>(data).get());
            break;
        case value::value_kind::int8:
            out.write_int(takatori::util::unsafe_downcast<value::int8>(data).get());
            break;
        case value::value_kind::float4:
            out.write_float4(takatori::util::unsafe_downcast<value::float4>(data).get());
            break;
        case value::value_kind::float8:
            out.write_float8(takatori::util::unsafe_downcast<value::float8>(data).get());
            break;
        case value::value_kind::decimal:
            out.write_decimal(takatori::util::unsafe_downcast<value::decimal>(data).get());
            break;
        case value::value_kind::character:
            out.write_character(takatori::util::unsafe_downcast<value::character>(data).get());
            break;
        case value::value_kind::octet:
            out.write_octet(takatori::util::unsafe_downcast<value::octet>(data).get());
            break;

        case value::value_kind::date:
            out.write_date(takatori::util::unsafe_downcast<value::date>(data).get());
            break;
        case value::value_kind::time_of_day:
            out.write_time_of_day(takatori::util::unsafe_downcast<value::time_of_day>(data).get());
            break;
        case value::value_kind::time_point:
            out.write_time_point(takatori::util::unsafe_downcast<value::time_point>(data).get());
            break;
        case value::value_kind::unknown:
            if (takatori::util::unsafe_downcast<value::unknown>(data).get() != value::unknown_kind::null) {
                throw std::invalid_argument("only NULL unknown literals are supported");
            }
            out.write_null();
            break;
        default:
            throw std::invalid_argument("unsupported remote execution literal");
    }
    return buffer.release();
}
std::unique_ptr<takatori::value::data> decode_remote_literal(std::string_view payload,
                                                             takatori::type::data const& type) {
    if (type.kind() == takatori::type::type_kind::int1 || type.kind() == takatori::type::type_kind::int2 ||
        type.kind() == takatori::type::type_kind::bit || type.kind() == takatori::type::type_kind::datetime_interval) {
        return {};
    }
    namespace value = takatori::value;
    using k = takatori::type::type_kind;
    auto* pos = payload.data();
    auto* end = payload.data();
    std::advance(end, payload.size());
    auto read_integer = [&](std::int64_t min, std::int64_t max) {
        auto value = serializer::read_int(pos, end);
        if (value < min || value > max) {
            throw std::invalid_argument("remote literal integer overflow");
        }
        return value;
    };
    std::unique_ptr<value::data> result{};
    try {
        if (serializer::peek_type(pos, end) == serializer::entry_type::null) {
            serializer::read_null(pos, end);
            result = std::make_unique<value::unknown>(value::unknown_kind::null);
        } else {
            switch (type.kind()) {
                case k::boolean:
                    result = std::make_unique<value::boolean>((read_integer(0, 1) != 0));
                    break;
                case k::int4:
                    result = std::make_unique<value::int4>(static_cast<std::int32_t>(read_integer(
                        std::numeric_limits<std::int32_t>::min(), std::numeric_limits<std::int32_t>::max())));
                    break;
                case k::int8:
                    result = std::make_unique<value::int8>(serializer::read_int(pos, end));
                    break;
                case k::float4:
                    result = std::make_unique<value::float4>(serializer::read_float4(pos, end));
                    break;
                case k::float8:
                    result = std::make_unique<value::float8>(serializer::read_float8(pos, end));
                    break;
                case k::decimal:
                    result = std::make_unique<value::decimal>(serializer::read_decimal(pos, end));
                    break;
                case k::character:
                    result = std::make_unique<value::character>(serializer::read_character(pos, end));
                    break;
                case k::octet:
                    result = std::make_unique<value::octet>(serializer::read_octet(pos, end));
                    break;

                case k::date:
                    result = std::make_unique<value::date>(serializer::read_date(pos, end));
                    break;
                case k::time_of_day:
                    result = std::make_unique<value::time_of_day>(serializer::read_time_of_day(pos, end));
                    break;
                case k::time_point:
                    result = std::make_unique<value::time_point>(serializer::read_time_point(pos, end));
                    break;
                default:
                    return {};
            }
        }
    } catch (std::exception const&) {
        return {};
    }
    if (pos != end) {
        return {};
    }
    return result;
}
} // namespace jogasaki::dist
