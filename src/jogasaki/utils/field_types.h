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
#pragma once

#include <cstddef>

#include <yugawara/compiled_info.h>
#include <takatori/type/type_kind.h>
#include <takatori/descriptor/variable.h>

#include <jogasaki/constants.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::utils {

using ::takatori::util::fail;
using ::takatori::util::enum_tag_t;

inline meta::field_type type_for(yugawara::compiled_info const& info, ::takatori::descriptor::variable const& var) {
    auto const& type = info.type_of(var);
    using t = takatori::type::type_kind;
    using k = meta::field_type_kind;
    switch(type.kind()) {
        case t::boolean: return meta::field_type(takatori::util::enum_tag<k::boolean>);
        case t::int1: return meta::field_type(takatori::util::enum_tag<k::int1>);
        case t::int2: return meta::field_type(takatori::util::enum_tag<k::int2>);
        case t::int4: return meta::field_type(takatori::util::enum_tag<k::int4>);
        case t::int8: return meta::field_type(takatori::util::enum_tag<k::int8>);
        case t::float4: return meta::field_type(takatori::util::enum_tag<k::float4>);
        case t::float8: return meta::field_type(takatori::util::enum_tag<k::float8>);
        case t::decimal: return meta::field_type(takatori::util::enum_tag<k::decimal>);
        case t::character: return meta::field_type(takatori::util::enum_tag<k::character>);
        case t::bit: return meta::field_type(takatori::util::enum_tag<k::bit>);
        case t::date: takatori::util::fail();
        case t::time_of_day: return meta::field_type(takatori::util::enum_tag<k::time_of_day>);
        case t::time_point: takatori::util::fail();
        case t::datetime_interval: return meta::field_type(takatori::util::enum_tag<k::time_interval>);

        case t::array:
        case t::record:
        case t::unknown:
        case t::row_reference:
        case t::row_id:
        case t::declared:
        case t::extension:
            fail();
    }
    fail();
}

}

