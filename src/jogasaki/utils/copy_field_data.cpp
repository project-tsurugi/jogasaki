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
#include "copy_field_data.h"

#include <cstddef>

#include <takatori/util/fail.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/data/any.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::utils {

using takatori::util::fail;

void copy_field(
    const meta::field_type &type,
    accessor::record_ref target,
    std::size_t target_offset,
    accessor::record_ref source,
    std::size_t source_offset,
    memory::paged_memory_resource* resource
) {
    using k = meta::field_type_kind;
    switch(type.kind()) {
        case k::undefined:
            break;
        case k::boolean: target.set_value(target_offset, source.get_value<runtime_t<k::boolean>>(source_offset)); return;
        case k::int1: target.set_value(target_offset, source.get_value<runtime_t<k::int1>>(source_offset)); return;
        case k::int2: target.set_value(target_offset, source.get_value<runtime_t<k::int2>>(source_offset)); return;
        case k::int4: target.set_value(target_offset, source.get_value<runtime_t<k::int4>>(source_offset)); return;
        case k::int8: target.set_value(target_offset, source.get_value<runtime_t<k::int8>>(source_offset)); return;
        case k::float4: target.set_value(target_offset, source.get_value<runtime_t<k::float4>>(source_offset)); return;
        case k::float8: target.set_value(target_offset, source.get_value<runtime_t<k::float8>>(source_offset)); return;
        case k::decimal: target.set_value(target_offset, source.get_value<runtime_t<k::decimal>>(source_offset)); return;
        case k::character: {
            auto text = source.get_value<runtime_t<k::character>>(source_offset);
            target.set_value(target_offset,
                resource != nullptr ? accessor::text(resource, text) : text
            );
            return;
        }
        case k::octet: {
            auto bin = source.get_value<runtime_t<k::octet>>(source_offset);
            target.set_value(target_offset,
                resource != nullptr ? accessor::binary(resource, bin) : bin
            );
            return;
        }
        case k::bit:
            break;
        case k::date: target.set_value(target_offset, source.get_value<runtime_t<k::date>>(source_offset)); return;
        case k::time_of_day: target.set_value(target_offset, source.get_value<runtime_t<k::time_of_day>>(source_offset)); return;
        case k::time_point: target.set_value(target_offset, source.get_value<runtime_t<k::time_point>>(source_offset)); return;
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
        case k::pointer: target.set_value(target_offset, source.get_value<runtime_t<k::pointer>>(source_offset)); return;
        case k::reference_column_position:
            break;
        case k::reference_column_name:
            break;
    }
    fail();
}

void copy_nullable_field(
    meta::field_type const& type,
    accessor::record_ref target,
    std::size_t target_offset,
    std::size_t target_nullity_offset,
    accessor::record_ref source,
    std::size_t source_offset,
    std::size_t source_nullity_offset,
    memory::paged_memory_resource* resource
) {
    bool is_null = source.is_null(source_nullity_offset);
    target.set_null(target_nullity_offset, is_null);
    if (is_null) return;
    copy_field(type, target, target_offset, source, source_offset, resource);
}

void copy_field(
    meta::field_type const& type,
    accessor::record_ref target,
    std::size_t target_offset,
    data::any const& source,
    memory::paged_memory_resource* resource
) {
    using k = meta::field_type_kind;
    switch(type.kind()) {
        case k::undefined:
            break;
        case k::boolean: target.set_value(target_offset, source.to<runtime_t<k::boolean>>()); return;
        case k::int1: target.set_value(target_offset, source.to<runtime_t<k::int1>>()); return;
        case k::int2: target.set_value(target_offset, source.to<runtime_t<k::int2>>()); return;
        case k::int4: target.set_value(target_offset, source.to<runtime_t<k::int4>>()); return;
        case k::int8: target.set_value(target_offset, source.to<runtime_t<k::int8>>()); return;
        case k::float4: target.set_value(target_offset, source.to<runtime_t<k::float4>>()); return;
        case k::float8: target.set_value(target_offset, source.to<runtime_t<k::float8>>()); return;
        case k::decimal: target.set_value(target_offset, source.to<runtime_t<k::decimal>>()); return;
        case k::character: {
            auto text = source.to<runtime_t<k::character>>();
            target.set_value(target_offset,
                resource != nullptr ? accessor::text(resource, text) : text
            );
            return;
        }
        case k::octet: {
            auto bin = source.to<runtime_t<k::octet>>();
            target.set_value(target_offset,
                resource != nullptr ? accessor::binary(resource, bin) : bin
            );
            return;
        }
        case k::bit:
            break;
        case k::date: target.set_value(target_offset, source.to<runtime_t<k::date>>()); return;
        case k::time_of_day: target.set_value(target_offset, source.to<runtime_t<k::time_of_day>>()); return;
        case k::time_point: target.set_value(target_offset, source.to<runtime_t<k::time_point>>()); return;
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
        case k::pointer:
            fail();
        case k::reference_column_position:
            fail();
        case k::reference_column_name:
            fail();
    }
    fail();
}

void copy_nullable_field(
    meta::field_type const& type,
    accessor::record_ref target,
    std::size_t target_offset,
    std::size_t target_nullity_offset,
    data::any const& source,
    memory::paged_memory_resource* resource
) {
    bool is_null = source.empty();
    target.set_null(target_nullity_offset, is_null);
    if (is_null) return;
    copy_field(type, target, target_offset, source, resource);
}

}

