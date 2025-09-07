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
#include "common_column_utils.h"

#include <variant>

namespace jogasaki::executor::dto {

namespace proto = jogasaki::proto;

common_column::atom_type from(proto::sql::common::AtomType v) noexcept {
	using proto = proto::sql::common::AtomType;
    using atom_type = common_column::atom_type;
	switch (v) {
		case proto::TYPE_UNSPECIFIED: return atom_type::type_unspecified;
		case proto::BOOLEAN: return atom_type::boolean;
		case proto::INT4: return atom_type::int4;
		case proto::INT8: return atom_type::int8;
		case proto::FLOAT4: return atom_type::float4;
		case proto::FLOAT8: return atom_type::float8;
		case proto::DECIMAL: return atom_type::decimal;
		case proto::CHARACTER: return atom_type::character;
		case proto::OCTET: return atom_type::octet;
		case proto::BIT: return atom_type::bit;
		case proto::DATE: return atom_type::date;
		case proto::TIME_OF_DAY: return atom_type::time_of_day;
		case proto::TIME_POINT: return atom_type::time_point;
		case proto::DATETIME_INTERVAL: return atom_type::datetime_interval;
		case proto::TIME_OF_DAY_WITH_TIME_ZONE: return atom_type::time_of_day_with_time_zone;
		case proto::TIME_POINT_WITH_TIME_ZONE: return atom_type::time_point_with_time_zone;
		case proto::CLOB: return atom_type::clob;
		case proto::BLOB: return atom_type::blob;
		case proto::UNKNOWN: return atom_type::unknown;
		default: return atom_type::type_unspecified;
	}
    std::abort();
}

proto::sql::common::AtomType from(common_column::atom_type v) noexcept {
	using proto = proto::sql::common::AtomType;
    using atom_type = common_column::atom_type;
	switch (v) {
		case atom_type::type_unspecified: return proto::TYPE_UNSPECIFIED;
		case atom_type::boolean: return proto::BOOLEAN;
		case atom_type::int4: return proto::INT4;
		case atom_type::int8: return proto::INT8;
		case atom_type::float4: return proto::FLOAT4;
		case atom_type::float8: return proto::FLOAT8;
		case atom_type::decimal: return proto::DECIMAL;
		case atom_type::character: return proto::CHARACTER;
		case atom_type::octet: return proto::OCTET;
		case atom_type::bit: return proto::BIT;
		case atom_type::date: return proto::DATE;
		case atom_type::time_of_day: return proto::TIME_OF_DAY;
		case atom_type::time_point: return proto::TIME_POINT;
		case atom_type::datetime_interval: return proto::DATETIME_INTERVAL;
		case atom_type::time_of_day_with_time_zone: return proto::TIME_OF_DAY_WITH_TIME_ZONE;
		case atom_type::time_point_with_time_zone: return proto::TIME_POINT_WITH_TIME_ZONE;
		case atom_type::clob: return proto::CLOB;
		case atom_type::blob: return proto::BLOB;
		case atom_type::unknown: return proto::UNKNOWN;
	}
    std::abort();
}

// oneof presence helpers
bool has_atom_type(proto::sql::common::Column const& msg) noexcept {
	return msg.type_info_case() == proto::sql::common::Column::kAtomType;
}

bool has_length(proto::sql::common::Column const& msg) noexcept {
	return msg.length_opt_case() == proto::sql::common::Column::kLength;
}

bool has_arbitrary_length(proto::sql::common::Column const& msg) noexcept {
	return msg.length_opt_case() == proto::sql::common::Column::kArbitraryLength;
}

bool has_precision(proto::sql::common::Column const& msg) noexcept {
	return msg.precision_opt_case() == proto::sql::common::Column::kPrecision;
}

bool has_arbitrary_precision(proto::sql::common::Column const& msg) noexcept {
	return msg.precision_opt_case() == proto::sql::common::Column::kArbitraryPrecision;
}

bool has_scale(proto::sql::common::Column const& msg) noexcept {
	return msg.scale_opt_case() == proto::sql::common::Column::kScale;
}

bool has_arbitrary_scale(proto::sql::common::Column const& msg) noexcept {
	return msg.scale_opt_case() == proto::sql::common::Column::kArbitraryScale;
}

bool has_nullable(proto::sql::common::Column const& msg) noexcept {
	return msg.nullable_opt_case() == proto::sql::common::Column::kNullable;
}

bool has_varying(proto::sql::common::Column const& msg) noexcept {
	return msg.varying_opt_case() == proto::sql::common::Column::kVarying;
}

bool has_description(proto::sql::common::Column const& msg) noexcept {
	return msg.description_opt_case() == proto::sql::common::Column::kDescription;
}

common_column from_proto(proto::sql::common::Column const& src) {
	common_column out{};
	if (! src.name().empty()) {
		out.name_ = src.name();
	}
	// currently only atom_type is used and row type and user type are not supported
	if (has_atom_type(src)) {
		out.atom_type_ = from(src.atom_type());
	}
	out.dimension_ = src.dimension();

	if (has_length(src)) {
		out.length_ = std::variant<std::uint32_t, bool>{src.length()};
	} else if (has_arbitrary_length(src)) {
		out.length_ = std::variant<std::uint32_t, bool>{true};
	}

	if (has_precision(src)) {
		out.precision_ = std::variant<std::uint32_t, bool>{src.precision()};
	} else if (has_arbitrary_precision(src)) {
		out.precision_ = std::variant<std::uint32_t, bool>{true};
	}

	if (has_scale(src)) {
		out.scale_ = std::variant<std::uint32_t, bool>{src.scale()};
	} else if (has_arbitrary_scale(src)) {
		out.scale_ = std::variant<std::uint32_t, bool>{true};
	}

	if (has_nullable(src)) {
		out.nullable_ = src.nullable();
	}
	if (has_varying(src)) {
		out.varying_ = src.varying();
	}
	if (has_description(src)) {
		out.description_ = src.description();
	}

	return out;
}

proto::sql::common::Column to_proto(common_column const& src) {
	proto::sql::common::Column out{};
	if (! src.name_.empty()) {
		out.set_name(src.name_);
	}
	out.set_atom_type(from(src.atom_type_));
	out.set_dimension(src.dimension_);

	if (src.length_) {
		if (std::holds_alternative<std::uint32_t>(*src.length_)) {
			out.set_length(std::get<std::uint32_t>(*src.length_));
		} else {
			// arbitrary
			out.mutable_arbitrary_length();
		}
	}

	if (src.precision_) {
		if (std::holds_alternative<std::uint32_t>(*src.precision_)) {
			out.set_precision(std::get<std::uint32_t>(*src.precision_));
		} else {
			out.mutable_arbitrary_precision();
		}
	}

	if (src.scale_) {
		if (std::holds_alternative<std::uint32_t>(*src.scale_)) {
			out.set_scale(std::get<std::uint32_t>(*src.scale_));
		} else {
			out.mutable_arbitrary_scale();
		}
	}

	if (src.nullable_) {
		out.set_nullable(*src.nullable_);
	}
	if (src.varying_) {
		out.set_varying(*src.varying_);
	}
	if (src.description_) {
		out.set_description(*src.description_);
	}

	return out;
}

} // namespace jogasaki::executor

