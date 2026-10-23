/*
 * Copyright 2026 Project Tsurugi.
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
#include "field_operations.h"
#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::data {
template<class T> static T copy_value(T value, memory::paged_memory_resource* resource) {
    if constexpr (std::is_same_v<T, accessor::text> || std::is_same_v<T, accessor::binary>) {
        if (resource) {
            return T{resource, value};
        }
    }
    return value;
}
template<class T> static void copy_record(accessor::record_ref target, std::size_t target_offset,
    accessor::const_record_ref source, std::size_t source_offset, memory::paged_memory_resource* resource) {
    target.set_value(target_offset, copy_value(source.get_value<T>(source_offset), resource));
}
template<class T> static void write_record(accessor::record_ref target, std::size_t offset,
    any const& source, memory::paged_memory_resource* resource) {
    target.set_value(offset, copy_value(source.to<T>(), resource));
}
template<class T> static any read_record(accessor::const_record_ref source, std::size_t offset,
    memory::paged_memory_resource* resource) {
    return any{std::in_place_type<T>, copy_value(source.get_value<T>(offset), resource)};
}
template<class T>
field_operations field_operations::for_value_type() {
    return {copy_record<T>, write_record<T>, read_record<T>, any::index<T>};
}
field_operations field_operations::for_type(meta::field_type const& type) {
    using k = meta::field_type_kind;
    // This is the only physical-type dispatch for reads, writes, and record copies.
    switch (type.kind()) {
        case k::boolean: return for_value_type<runtime_t<k::boolean>>();
        case k::int1: return for_value_type<runtime_t<k::int1>>();
        case k::int2: return for_value_type<runtime_t<k::int2>>();
        case k::int4: return for_value_type<runtime_t<k::int4>>();
        case k::int8: return for_value_type<runtime_t<k::int8>>();
        case k::float4: return for_value_type<runtime_t<k::float4>>();
        case k::float8: return for_value_type<runtime_t<k::float8>>();
        case k::decimal: return for_value_type<runtime_t<k::decimal>>();
        case k::character: return for_value_type<runtime_t<k::character>>();
        case k::octet: return for_value_type<runtime_t<k::octet>>();
        case k::date: return for_value_type<runtime_t<k::date>>();
        case k::time_of_day: return for_value_type<runtime_t<k::time_of_day>>();
        case k::time_point: return for_value_type<runtime_t<k::time_point>>();
        case k::blob: return for_value_type<runtime_t<k::blob>>();
        case k::clob: return for_value_type<runtime_t<k::clob>>();
        case k::pointer: return {copy_record<runtime_t<k::pointer>>, nullptr, nullptr, static_cast<std::size_t>(-1)};
        case k::unknown: return {nullptr, nullptr, nullptr, any::index<std::monostate>};
        default: fail_with_exception();
    }
}
void field_operations::copy(accessor::record_ref target, std::size_t target_offset,
    accessor::const_record_ref source, std::size_t source_offset, memory::paged_memory_resource* resource) const {
    if (!copy_) {
        fail_with_exception();
    }
    copy_(target, target_offset, source, source_offset, resource);
}
void field_operations::copy(accessor::record_ref target, std::size_t target_offset,
    any const& source, memory::paged_memory_resource* resource) const {
    if (!write_) {
        fail_with_exception();
    }
    write_(target, target_offset, source, resource);
}
void field_operations::copy_nullable(accessor::record_ref target, std::size_t target_offset, std::size_t target_nullity,
    accessor::const_record_ref source, std::size_t source_offset, std::size_t source_nullity,
    memory::paged_memory_resource* resource) const {
    if (source.is_null(source_nullity)) { target.set_null(target_nullity, true); return; }
    copy(target, target_offset, source, source_offset, resource);
    target.set_null(target_nullity, false);
}
void field_operations::copy_nullable(accessor::record_ref target, std::size_t target_offset, std::size_t target_nullity,
    any const& source, memory::paged_memory_resource* resource) const {
    if (source.empty()) { target.set_null(target_nullity, true); return; }
    copy(target, target_offset, source, resource);
    target.set_null(target_nullity, false);
}
void field_operations::read(accessor::const_record_ref source, std::size_t source_offset, any& result,
    memory::paged_memory_resource* resource) const {
    if (!read_) {
        fail_with_exception();
    }
    result = read_(source, source_offset, resource);
}
void field_operations::read_nullable(accessor::const_record_ref source, std::size_t source_offset,
    std::size_t source_nullity, any& result, memory::paged_memory_resource* resource) const {
    if (source.is_null(source_nullity)) { result = {}; return; }
    read(source, source_offset, result, resource);
}
}
