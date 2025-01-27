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
#include "parameter_set.h"

#include <memory>
#include <utility>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>

#include <jogasaki/data/binary_string_value.h>
#include <jogasaki/data/value.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/plan/parameter_entry.h>

namespace jogasaki::plan {

using kind = meta::field_type_kind;

void parameter_set::set_boolean(std::string_view name, parameter_t<kind::boolean> value) {
    add(std::string(name),
        {
            meta::field_type{meta::field_enum_tag<kind::boolean>},
            data::value{std::in_place_type<parameter_t<kind::boolean>>, value}
        }
    );
}

void parameter_set::set_int4(std::string_view name, parameter_t<kind::int4> value) {
    add(std::string(name),
        {
            meta::field_type{meta::field_enum_tag<kind::int4>},
            data::value{std::in_place_type<parameter_t<kind::int4>>, value}
        }
    );
}

void parameter_set::set_int8(std::string_view name, parameter_t<kind::int8> value) {
    add(std::string(name),
        {
            meta::field_type{meta::field_enum_tag<kind::int8>},
            data::value{std::in_place_type<parameter_t<kind::int8>>, value}
        }
    );
}

void parameter_set::set_float4(std::string_view name, parameter_t<kind::float4> value) {
    add(std::string(name),
        {
            meta::field_type{meta::field_enum_tag<kind::float4>},
            data::value{std::in_place_type<parameter_t<kind::float4>>, value}
        }
    );
}

void parameter_set::set_float8(std::string_view name, parameter_t<kind::float8> value) {
    add(std::string(name),
        {
            meta::field_type{meta::field_enum_tag<kind::float8>},
            data::value{std::in_place_type<parameter_t<kind::float8>>, value}
        }
    );
}

void parameter_set::set_character(std::string_view name, parameter_t<kind::character> value) {
    add(std::string(name),
        {
            meta::field_type{std::make_shared<meta::character_field_option>()},
            data::value{std::in_place_type<std::string>, static_cast<std::string_view>(value)}
            // use std::string so that the content is copied from accessor::text
        }
    );
}

void parameter_set::set_octet(std::string_view name, parameter_t<kind::octet> value) {
    add(std::string(name),
        {
            meta::field_type{std::make_shared<meta::octet_field_option>()},
            data::value{std::in_place_type<data::binary_string_value>, static_cast<std::string_view>(value)}
            // use binary_string_value so that the content is copied from accessor::binary
        }
    );
}
void parameter_set::set_decimal(std::string_view name, parameter_t<kind::decimal> value) {
    add(std::string(name),
        {
            meta::field_type{std::make_shared<meta::decimal_field_option>()},
            data::value{std::in_place_type<parameter_t<kind::decimal>>, value}
        }
    );
}

void parameter_set::set_date(std::string_view name, parameter_t<kind::date> value) {
    add(std::string(name),
        {
            meta::field_type{meta::field_enum_tag<kind::date>},
            data::value{std::in_place_type<parameter_t<kind::date>>, value}
        }
    );
}

void parameter_set::set_time_of_day(std::string_view name, parameter_t<kind::time_of_day> value) {
    add(std::string(name),
        {
            meta::field_type{std::make_shared<meta::time_of_day_field_option>()},
            data::value{std::in_place_type<parameter_t<kind::time_of_day>>, value}
        }
    );
}

void parameter_set::set_time_point(std::string_view name, parameter_t<kind::time_point> value) {
    add(std::string(name),
        {
            meta::field_type{std::make_shared<meta::time_point_field_option>()},
            data::value{std::in_place_type<parameter_t<kind::time_point>>, value}
        }
    );
}

void parameter_set::set_reference_column(std::string_view name, std::size_t position) {
    add(std::string(name),
        {
            meta::field_type{meta::field_enum_tag<kind::reference_column_position>},
            data::value{std::in_place_type<std::size_t>, position}
        }
    );
}

void parameter_set::set_reference_column(std::string_view name, std::string_view column_name) {
    add(std::string(name),
        {
            meta::field_type{meta::field_enum_tag<kind::reference_column_name>},
            data::value{std::in_place_type<std::string>, column_name}
        }
    );
}

void parameter_set::set_null(std::string_view name) {
    add(std::string(name),
        {
            meta::field_type{meta::field_enum_tag<kind::undefined>},
            data::value{}
        }
    );
}

optional_ptr<parameter_set::entry_type const> parameter_set::find(std::string_view name) const {
    if (map_.count(std::string(name)) != 0) {
        return map_.at(std::string(name));
    }
    return {};
}

std::size_t parameter_set::size() const noexcept {
    return map_.size();
}

void parameter_set::add(std::string name, parameter_set::entry_type entry) {
    map_.insert_or_assign(std::move(name), std::move(entry));
}

parameter_set::iterator parameter_set::begin() const noexcept {
    return map_.begin();
}

parameter_set::iterator parameter_set::end() const noexcept {
    return map_.end();
}

}
