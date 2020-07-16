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

#include <takatori/util/fail.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::utils {

using ::takatori::util::fail;

inline void copy_field(
    meta::field_type const& type,
    accessor::record_ref target,
    std::size_t target_offset,
    accessor::record_ref source,
    std::size_t source_offset
) {
    using k = meta::field_type_kind;
    switch(type.kind()) {
        case k::undefined:
        case k::boolean:
        case k::int1:
        case k::int2:
            break;
        case k::int4: target.set_value(target_offset, source.get_value<meta::field_type_traits<k::int4>::runtime_type>(source_offset)); return;
        case k::int8: target.set_value(target_offset, source.get_value<meta::field_type_traits<k::int8>::runtime_type>(source_offset)); return;
        case k::float4: target.set_value(target_offset, source.get_value<meta::field_type_traits<k::float4>::runtime_type>(source_offset)); return;
        case k::float8: target.set_value(target_offset, source.get_value<meta::field_type_traits<k::float8>::runtime_type>(source_offset)); return;
        case k::decimal:
            break;
        case k::character:
            break;
        case k::bit:
            break;
        case k::date:
            break;
        case k::time_of_day:
            break;
        case k::time_point:
            break;
        case k::time_interval:
            break;
        case k::array:
            break;
        case k::record:
            break;
        case k::unknown:
            break;
        case k::row_reference:
            break;
        case k::row_id:
            break;
        case k::declared:
            break;
        case k::extension:
            break;
    }
    fail();
}

}

