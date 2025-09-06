/*
 * Copyright 2018-2025 Project Tsurugi.
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
#pragma once

#include <cstdint>
#include <optional>

#include <google/protobuf/empty.pb.h>

#include <jogasaki/executor/common_column.h>
#include <jogasaki/proto/sql/common.pb.h>

namespace jogasaki::executor {

/**
 * @brief convert proto AtomType to executor::common_column::atom_type.
 */
inline common_column::atom_type from_proto(jogasaki::proto::sql::common::AtomType v) noexcept {
    using proto = jogasaki::proto::sql::common::AtomType;
    switch (v) {
        case proto::TYPE_UNSPECIFIED: return common_column::atom_type::type_unspecified;
        case proto::BOOLEAN: return common_column::atom_type::boolean;
        case proto::INT4: return common_column::atom_type::int4;
        case proto::INT8: return common_column::atom_type::int8;
        case proto::FLOAT4: return common_column::atom_type::float4;
        case proto::FLOAT8: return common_column::atom_type::float8;
        case proto::DECIMAL: return common_column::atom_type::decimal;
        case proto::CHARACTER: return common_column::atom_type::character;
        case proto::OCTET: return common_column::atom_type::octet;
        case proto::BIT: return common_column::atom_type::bit;
        case proto::DATE: return common_column::atom_type::date;
        case proto::TIME_OF_DAY: return common_column::atom_type::time_of_day;
        case proto::TIME_POINT: return common_column::atom_type::time_point;
        case proto::DATETIME_INTERVAL: return common_column::atom_type::datetime_interval;
        case proto::TIME_OF_DAY_WITH_TIME_ZONE: return common_column::atom_type::time_of_day_with_time_zone;
        case proto::TIME_POINT_WITH_TIME_ZONE: return common_column::atom_type::time_point_with_time_zone;
        case proto::CLOB: return common_column::atom_type::clob;
        case proto::BLOB: return common_column::atom_type::blob;
        case proto::UNKNOWN: return common_column::atom_type::unknown;
        default: return common_column::atom_type::type_unspecified;
    }
}

/**
 * @brief convert executor::common_column::atom_type to proto AtomType.
 */
inline jogasaki::proto::sql::common::AtomType to_proto(common_column::atom_type v) noexcept {
    using proto = jogasaki::proto::sql::common::AtomType;
    switch (v) {
        case common_column::atom_type::type_unspecified: return proto::TYPE_UNSPECIFIED;
        case common_column::atom_type::boolean: return proto::BOOLEAN;
        case common_column::atom_type::int1: return proto::TYPE_UNSPECIFIED;
        case common_column::atom_type::int2: return proto::TYPE_UNSPECIFIED;
        // int1/int2 map to unspecified in SQL proto AtomType
        case common_column::atom_type::int4: return proto::INT4;
        case common_column::atom_type::int8: return proto::INT8;
        case common_column::atom_type::float4: return proto::FLOAT4;
        case common_column::atom_type::float8: return proto::FLOAT8;
        case common_column::atom_type::decimal: return proto::DECIMAL;
        case common_column::atom_type::character: return proto::CHARACTER;
        case common_column::atom_type::octet: return proto::OCTET;
        case common_column::atom_type::bit: return proto::BIT;
        case common_column::atom_type::date: return proto::DATE;
        case common_column::atom_type::time_of_day: return proto::TIME_OF_DAY;
        case common_column::atom_type::time_point: return proto::TIME_POINT;
        case common_column::atom_type::datetime_interval: return proto::DATETIME_INTERVAL;
        case common_column::atom_type::time_of_day_with_time_zone: return proto::TIME_OF_DAY_WITH_TIME_ZONE;
        case common_column::atom_type::time_point_with_time_zone: return proto::TIME_POINT_WITH_TIME_ZONE;
        case common_column::atom_type::clob: return proto::CLOB;
        case common_column::atom_type::blob: return proto::BLOB;
        case common_column::atom_type::unknown: return proto::UNKNOWN;
    }
    return proto::TYPE_UNSPECIFIED;
}

/**
 * @brief convert proto Column message to common_column.
 */
inline common_column from_proto(jogasaki::proto::sql::common::Column const& src) {
    common_column out{};
    if (!src.name().empty()) {
        out.name_ = src.name();
    }
    // type: assume atom_type only as requested
    if (src.has_atom_type()) {
        out.atom_type_ = from_proto(src.atom_type());
    }
    out.dimension_ = src.dimension();

    // length
    if (src.has_length()) {
        out.length_opt_ = std::variant<std::uint32_t, bool>{src.length()};
    } else if (src.has_arbitrary_length()) {
        out.length_opt_ = std::variant<std::uint32_t, bool>{true};
    }

    // precision
    if (src.has_precision()) {
        out.precision_opt_ = std::variant<std::uint32_t, bool>{src.precision()};
    } else if (src.has_arbitrary_precision()) {
        out.precision_opt_ = std::variant<std::uint32_t, bool>{true};
    }

    // scale
    if (src.has_scale()) {
        out.scale_opt_ = std::variant<std::uint32_t, bool>{src.scale()};
    } else if (src.has_arbitrary_scale()) {
        out.scale_opt_ = std::variant<std::uint32_t, bool>{true};
    }

    if (src.has_nullable()) {
        out.nullable_opt_ = src.nullable();
    }
    if (src.has_varying()) {
        out.varying_opt_ = src.varying();
    }
    if (src.has_description()) {
        out.description_ = src.description();
    }

    return out;
}

/**
 * @brief convert common_column to proto Column message.
 */
inline jogasaki::proto::sql::common::Column to_proto(common_column const& src) {
    jogasaki::proto::sql::common::Column out{};
    if (!src.name_.empty()) {
        out.set_name(src.name_);
    }
    out.set_atom_type(to_proto(src.atom_type_));
    out.set_dimension(src.dimension_);

    if (src.length_opt_) {
        if (std::holds_alternative<std::uint32_t>(*src.length_opt_)) {
            out.set_length(std::get<std::uint32_t>(*src.length_opt_));
        } else {
            // arbitrary
            out.mutable_arbitrary_length();
        }
    }

    if (src.precision_opt_) {
        if (std::holds_alternative<std::uint32_t>(*src.precision_opt_)) {
            out.set_precision(std::get<std::uint32_t>(*src.precision_opt_));
        } else {
            out.mutable_arbitrary_precision();
        }
    }

    if (src.scale_opt_) {
        if (std::holds_alternative<std::uint32_t>(*src.scale_opt_)) {
            out.set_scale(std::get<std::uint32_t>(*src.scale_opt_));
        } else {
            out.mutable_arbitrary_scale();
        }
    }

    if (src.nullable_opt_) {
        out.set_nullable(*src.nullable_opt_);
    }
    if (src.varying_opt_) {
        out.set_varying(*src.varying_opt_);
    }
    if (src.description_) {
        out.set_description(*src.description_);
    }

    return out;
}

} // namespace jogasaki::executor
