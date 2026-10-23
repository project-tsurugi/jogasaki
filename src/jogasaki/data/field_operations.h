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
#pragma once
#include <jogasaki/accessor/const_record_ref.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/any.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>

namespace jogasaki::data {
/** Prepare physical field operations once, then use the same typed implementation for every direction. */
class field_operations {
public:
    field_operations() = default;
    [[nodiscard]] static field_operations for_type(meta::field_type const&);
    [[nodiscard]] bool accepts(any const& value) const noexcept { return index_ == value.type_index(); }
    void copy(accessor::record_ref target, std::size_t target_offset, accessor::const_record_ref source,
        std::size_t source_offset, memory::paged_memory_resource* resource = nullptr) const;
    void copy(accessor::record_ref target, std::size_t target_offset, any const& source,
        memory::paged_memory_resource* resource = nullptr) const;
    void copy_nullable(accessor::record_ref target, std::size_t target_offset, std::size_t target_nullity,
        accessor::const_record_ref source, std::size_t source_offset, std::size_t source_nullity,
        memory::paged_memory_resource* resource = nullptr) const;
    void copy_nullable(accessor::record_ref target, std::size_t target_offset, std::size_t target_nullity,
        any const& source, memory::paged_memory_resource* resource = nullptr) const;
    void read(accessor::const_record_ref source, std::size_t source_offset, any& result,
        memory::paged_memory_resource* resource = nullptr) const;
    void read_nullable(accessor::const_record_ref source, std::size_t source_offset, std::size_t source_nullity,
        any& result, memory::paged_memory_resource* resource = nullptr) const;
private:
    using copy_function = void (*)(accessor::record_ref, std::size_t, accessor::const_record_ref, std::size_t, memory::paged_memory_resource*);
    using write_function = void (*)(accessor::record_ref, std::size_t, any const&, memory::paged_memory_resource*);
    using read_function = any (*)(accessor::const_record_ref, std::size_t, memory::paged_memory_resource*);
    field_operations(copy_function copy, write_function write, read_function read, std::size_t index)
        : copy_(copy), write_(write), read_(read), index_(index) {}
    copy_function copy_{};
    write_function write_{};
    read_function read_{};
    std::size_t index_{static_cast<std::size_t>(-1)};
};
}
