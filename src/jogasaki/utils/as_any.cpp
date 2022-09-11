/*
 * Copyright 2018-2020 tsurugi project.
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
#include "as_any.h"

#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/value/character.h>
#include <takatori/value/decimal.h>
#include <takatori/value/data.h>
#include <takatori/value/time_of_day.h>
#include <takatori/value/time_point.h>
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/type/decimal.h>
#include <takatori/type/date.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/util/fail.h>

#include <jogasaki/data/any.h>

namespace jogasaki::utils {

using takatori::util::fail;

data::any as_any(
    takatori::value::data const& arg,
    takatori::type::data const& type,
    memory::paged_memory_resource* resource
) {
    using t = takatori::type::type_kind;
    switch(type.kind()) {
        // TODO create and use traits for types
        case t::boolean: return {std::in_place_type<bool>, value_of<takatori::value::boolean>(arg)};
        case t::int4: return {std::in_place_type<runtime_t<meta::field_type_kind::int4>>, value_of<takatori::value::int4>(arg)};
        case t::int8: return {std::in_place_type<runtime_t<meta::field_type_kind::int8>>, value_of<takatori::value::int8>(arg)};
        case t::float4: return {std::in_place_type<runtime_t<meta::field_type_kind::float4>>, value_of<takatori::value::float4>(arg)};
        case t::float8: return {std::in_place_type<runtime_t<meta::field_type_kind::float8>>, value_of<takatori::value::float8>(arg)};
        case t::character: {
            auto sv = value_of<takatori::value::character>(arg);
            return {
                std::in_place_type<accessor::text>,
                resource == nullptr ? accessor::text{sv} : accessor::text{resource, sv}
            };
        }
        case t::decimal: return {std::in_place_type<runtime_t<meta::field_type_kind::decimal>>, value_of<takatori::value::decimal>(arg)};
        case t::date: return {std::in_place_type<runtime_t<meta::field_type_kind::date>>, value_of<takatori::value::date>(arg)};
        case t::time_of_day: return {std::in_place_type<runtime_t<meta::field_type_kind::time_of_day>>, value_of<takatori::value::time_of_day>(arg)};
        case t::time_point: return {std::in_place_type<runtime_t<meta::field_type_kind::time_point>>, value_of<takatori::value::time_point>(arg)};
        case t::unknown: return {};
        default: fail();
    }
    fail();
}

}

