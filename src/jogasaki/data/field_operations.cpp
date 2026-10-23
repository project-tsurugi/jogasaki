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
namespace {
template<class T> T copy_value(T value, memory::paged_memory_resource* resource) {
    if constexpr (std::is_same_v<T, accessor::text> || std::is_same_v<T, accessor::binary>) {
        if (resource) return T{resource, value};
    }
    return value;
}
template<class T> void copy_record(accessor::record_ref target, std::size_t target_offset,
    accessor::const_record_ref source, std::size_t source_offset, memory::paged_memory_resource* resource) {
    target.set_value(target_offset, copy_value(source.get_value<T>(source_offset), resource));
}
template<class T> void write_record(accessor::record_ref target, std::size_t offset,
    any const& source, memory::paged_memory_resource* resource) {
    target.set_value(offset, copy_value(source.to<T>(), resource));
}
template<class T> any read_record(accessor::const_record_ref source, std::size_t offset,
    memory::paged_memory_resource* resource) {
    return any{std::in_place_type<T>, copy_value(source.get_value<T>(offset), resource)};
}
}
field_operations field_operations::for_type(meta::field_type const& type) {
    using k = meta::field_type_kind;
    // This is the only physical-type dispatch for reads, writes, and record copies.
#define FIELD_CASE(K) case k::K: return {copy_record<runtime_t<k::K>>, write_record<runtime_t<k::K>>, read_record<runtime_t<k::K>>, any::index<runtime_t<k::K>>}
    switch (type.kind()) {
        FIELD_CASE(boolean);
        FIELD_CASE(int1); FIELD_CASE(int2); FIELD_CASE(int4); FIELD_CASE(int8);
        FIELD_CASE(float4); FIELD_CASE(float8); FIELD_CASE(decimal);
        FIELD_CASE(character); FIELD_CASE(octet); FIELD_CASE(date);
        FIELD_CASE(time_of_day); FIELD_CASE(time_point); FIELD_CASE(blob); FIELD_CASE(clob);
        case k::pointer: return {copy_record<runtime_t<k::pointer>>, nullptr, nullptr, static_cast<std::size_t>(-1)};
        case k::unknown: return {nullptr, nullptr, nullptr, any::index<std::monostate>};
        default: fail_with_exception();
    }
#undef FIELD_CASE
}
void field_operations::copy(accessor::record_ref target, std::size_t target_offset,
    accessor::const_record_ref source, std::size_t source_offset, memory::paged_memory_resource* resource) const {
    if (!copy_) fail_with_exception();
    copy_(target, target_offset, source, source_offset, resource);
}
void field_operations::copy(accessor::record_ref target, std::size_t target_offset,
    any const& source, memory::paged_memory_resource* resource) const {
    if (!write_) fail_with_exception();
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
    if (!read_) fail_with_exception();
    result = read_(source, source_offset, resource);
}
void field_operations::read_nullable(accessor::const_record_ref source, std::size_t source_offset,
    std::size_t source_nullity, any& result, memory::paged_memory_resource* resource) const {
    if (source.is_null(source_nullity)) { result = {}; return; }
    read(source, source_offset, result, resource);
}
}
