/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include "field_type.h"

#include <utility>

#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::api::impl {

api::field_type_kind from(meta::field_type_kind k) noexcept {
    using kind = api::field_type_kind;
    switch(k) {
        case meta::field_type_kind::undefined: return kind::undefined;
        case meta::field_type_kind::boolean: return kind::boolean;
        case meta::field_type_kind::int1: return kind::int1;
        case meta::field_type_kind::int2: return kind::int2;
        case meta::field_type_kind::int4: return kind::int4;
        case meta::field_type_kind::int8: return kind::int8;
        case meta::field_type_kind::float4: return kind::float4;
        case meta::field_type_kind::float8: return kind::float8;
        case meta::field_type_kind::decimal: return kind::decimal;
        case meta::field_type_kind::character: return kind::character;
        case meta::field_type_kind::octet: return kind::octet;
        case meta::field_type_kind::bit: return kind::bit;
        case meta::field_type_kind::date: return kind::date;
        case meta::field_type_kind::time_of_day: return kind::time_of_day;
        case meta::field_type_kind::time_point: return kind::time_point;
        case meta::field_type_kind::time_interval: return kind::time_interval;
        case meta::field_type_kind::array: return kind::array;
        case meta::field_type_kind::record: return kind::record;
        case meta::field_type_kind::unknown: return kind::unknown;
        case meta::field_type_kind::row_reference: return kind::row_reference;
        case meta::field_type_kind::row_id: return kind::row_id;
        case meta::field_type_kind::declared: return kind::declared;
        case meta::field_type_kind::extension: return kind::extension;
        case meta::field_type_kind::reference_column_position: return kind::reference_column_position;
        case meta::field_type_kind::reference_column_name: return kind::reference_column_name;
        case meta::field_type_kind::pointer: return kind::pointer;
    }
    std::abort();
}

field_type::option_type create_option(meta::field_type const& type) noexcept {
    switch(type.kind()) {
        using t = decltype(type.kind());
        case t::character: {
            auto& opt = type.option_unsafe<meta::field_type_kind::character>();
            return std::make_shared<character_field_option>(opt->varying_, opt->length_);
        }
        case t::decimal: {
            auto& opt = type.option_unsafe<meta::field_type_kind::decimal>();
            return std::make_shared<decimal_field_option>(opt->precision_, opt->scale_);
        }
        case t::time_of_day: {
            auto& opt = type.option_unsafe<meta::field_type_kind::time_of_day>();
            return std::make_shared<time_of_day_field_option>(opt->with_offset_);
        }
        case t::time_point: {
            auto& opt = type.option_unsafe<meta::field_type_kind::time_point>();
            return std::make_shared<time_point_field_option>(opt->with_offset_);
        }
        default:
            return std::monostate{};
    }
    std::abort();
}

field_type::field_type(meta::field_type type) noexcept:
    type_(std::move(type)),
    option_(create_option(type_))
{}

api::field_type_kind field_type::kind() const noexcept {
    return from(type_.kind());
}

std::shared_ptr<character_field_option> const& field_type::character_option() const noexcept {
    static std::shared_ptr<character_field_option> nullopt{};
    if(type_.kind() != meta::field_type_kind::character) return nullopt;
    return *std::get_if<std::shared_ptr<character_field_option>>(std::addressof(option_));
}

std::shared_ptr<decimal_field_option> const& field_type::decimal_option() const noexcept {
    static std::shared_ptr<decimal_field_option> nullopt{};
    if(type_.kind() != meta::field_type_kind::decimal) return nullopt;
    return *std::get_if<std::shared_ptr<decimal_field_option>>(std::addressof(option_));
}

std::shared_ptr<time_of_day_field_option> const& field_type::time_of_day_option() const noexcept {
    static std::shared_ptr<time_of_day_field_option> nullopt{};
    if(type_.kind() != meta::field_type_kind::time_of_day) return nullopt;
    return *std::get_if<std::shared_ptr<time_of_day_field_option>>(std::addressof(option_));
}

std::shared_ptr<time_point_field_option> const& field_type::time_point_option() const noexcept {
    static std::shared_ptr<time_point_field_option> nullopt{};
    if(type_.kind() != meta::field_type_kind::time_point) return nullopt;
    return *std::get_if<std::shared_ptr<time_point_field_option>>(std::addressof(option_));
}

} // namespace

