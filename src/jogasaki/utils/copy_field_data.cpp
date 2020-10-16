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
#include "copy_field_data.h"

#include <cstddef>

#include <takatori/util/fail.h>

#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::utils {

using takatori::util::fail;

void copy_field(const meta::field_type &type, accessor::record_ref target, std::size_t target_offset,
    accessor::record_ref source, std::size_t source_offset, memory::paged_memory_resource* resource) {
    using k = meta::field_type_kind;
    switch(type.kind()) {
        case k::undefined:
            break;
        case k::boolean: target.set_value(target_offset, source.get_value<meta::field_type_traits<k::boolean>::runtime_type>(source_offset)); return;
        case k::int1: target.set_value(target_offset, source.get_value<meta::field_type_traits<k::int1>::runtime_type>(source_offset)); return;
        case k::int2: target.set_value(target_offset, source.get_value<meta::field_type_traits<k::int2>::runtime_type>(source_offset)); return;
        case k::int4: target.set_value(target_offset, source.get_value<meta::field_type_traits<k::int4>::runtime_type>(source_offset)); return;
        case k::int8: target.set_value(target_offset, source.get_value<meta::field_type_traits<k::int8>::runtime_type>(source_offset)); return;
        case k::float4: target.set_value(target_offset, source.get_value<meta::field_type_traits<k::float4>::runtime_type>(source_offset)); return;
        case k::float8: target.set_value(target_offset, source.get_value<meta::field_type_traits<k::float8>::runtime_type>(source_offset)); return;
        case k::decimal:
            break;
        case k::character: {
            auto text = source.get_value<meta::field_type_traits<k::character>::runtime_type>(source_offset);
            target.set_value(target_offset,
                resource != nullptr ? accessor::text(resource, text) : text
            );
            return;
        }
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

