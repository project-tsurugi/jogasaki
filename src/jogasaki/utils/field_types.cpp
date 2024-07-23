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
#include "field_types.h"

#include <memory>

#include <takatori/descriptor/variable.h>
#include <takatori/type/character.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/type_kind.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/octet_field_option.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::utils {

meta::field_type type_for(takatori::type::data const& type) {
    using t = takatori::type::type_kind;
    using k = meta::field_type_kind;
    switch(type.kind()) {
        case t::boolean: return meta::field_type(meta::field_enum_tag<k::boolean>);
        case t::int1: return meta::field_type(meta::field_enum_tag<k::int1>);
        case t::int2: return meta::field_type(meta::field_enum_tag<k::int2>);
        case t::int4: return meta::field_type(meta::field_enum_tag<k::int4>);
        case t::int8: return meta::field_type(meta::field_enum_tag<k::int8>);
        case t::float4: return meta::field_type(meta::field_enum_tag<k::float4>);
        case t::float8: return meta::field_type(meta::field_enum_tag<k::float8>);
        case t::decimal: {
            auto& typ = static_cast<::takatori::type::decimal const&>(type);  //NOLINT
            return meta::field_type(std::make_shared<meta::decimal_field_option>(typ.precision(), typ.scale()));
        }
        case t::character: {
            auto& typ = static_cast<::takatori::type::character const&>(type);  //NOLINT
            return meta::field_type(std::make_shared<meta::character_field_option>(typ.varying(), typ.length()));
        }
        case t::octet: {
            auto& typ = static_cast<::takatori::type::octet const&>(type);  //NOLINT
            return meta::field_type(std::make_shared<meta::octet_field_option>(typ.varying(), typ.length()));
        }
        case t::bit: return meta::field_type(meta::field_enum_tag<k::bit>);
        case t::date: return meta::field_type(meta::field_enum_tag<k::date>);
        case t::time_of_day: {
            auto& typ = static_cast<::takatori::type::time_of_day const&>(type);  //NOLINT
            bool with_offset = typ.with_time_zone();
            return meta::field_type(std::make_shared<meta::time_of_day_field_option>(with_offset));
        }
        case t::time_point: {
            auto& typ = static_cast<::takatori::type::time_point const&>(type);  //NOLINT
            bool with_offset = typ.with_time_zone();
            return meta::field_type(std::make_shared<meta::time_point_field_option>(with_offset));
        }
        case t::datetime_interval: return meta::field_type(meta::field_enum_tag<k::time_interval>);
        case t::unknown: return meta::field_type(meta::field_enum_tag<k::unknown>);

        case t::array:
        case t::record:
        case t::row_reference:
        case t::row_id:
        case t::declared:
        case t::extension:
            fail_with_exception();
    }
    std::abort();
}

meta::field_type type_for(yugawara::compiled_info const& info, takatori::descriptor::variable const& var) {
    auto const& type = info.type_of(var);
    return type_for(type);
}
}

