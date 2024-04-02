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
#pragma once

#include <cstddef>

#include <jogasaki/data/any.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/memory/paged_memory_resource.h>

namespace jogasaki::utils {

/**
 * @brief copy non-nullable record field data with given type
 * @param type type of the field being copied
 * @param target target record reference
 * @param target_offset byte offset of the target field in the target record
 * @param source source record reference
 * @param source_offset byte offset of the source field in the source record
 * @param resource memory resource optionally used to allocate varlen data on the target. Pass nullptr if no new
 * allocation is needed and the target still references the same varlen buffer as source.
 */
void copy_field(
    meta::field_type const& type,
    accessor::record_ref target,
    std::size_t target_offset,
    accessor::record_ref source,
    std::size_t source_offset,
    memory::paged_memory_resource* resource = nullptr
);

/**
 * @brief copy nullable record field data with given type
 * @param type type of the field being copied
 * @param target target record reference
 * @param target_offset byte offset of the target field in the target record
 * @param target_nullity_offset nullity bit offset of the target field
 * @param source source record reference
 * @param source_offset byte offset of the source field in the source record
 * @param source_nullity_offset nullity bit offset of the source field
 * @param resource memory resource optionally used to allocate varlen data on the target. Pass nullptr if no new
 * allocation is needed and the target still references the same varlen buffer as source.
 */
void copy_nullable_field(
    meta::field_type const& type,
    accessor::record_ref target,
    std::size_t target_offset,
    std::size_t target_nullity_offset,
    accessor::record_ref source,
    std::size_t source_offset,
    std::size_t source_nullity_offset,
    memory::paged_memory_resource* resource = nullptr
);

/**
 * @brief copy non-nullable record field data from any
 * @param type type of the field being copied
 * @param target target record reference
 * @param target_offset byte offset of the target field in the target record
 * @param source source field value contained in any
 * @param resource memory resource optionally used to allocate varlen data on the target. Pass nullptr if no new
 * allocation is needed and the target still references the same varlen buffer as source.
 */
void copy_field(
    meta::field_type const& type,
    accessor::record_ref target,
    std::size_t target_offset,
    data::any const& source,
    memory::paged_memory_resource* resource = nullptr
);

/**
 * @brief copy nullable record field data with given type
 * @param type type of the field being copied
 * @param target target record reference
 * @param target_offset byte offset of the target field in the target record
 * @param target_nullity_offset nullity bit offset of the target field
 * @param source source field value contained in any
 * @param resource memory resource optionally used to allocate varlen data on the target. Pass nullptr if no new
 * allocation is needed and the target still references the same varlen buffer as source.
 */
void copy_nullable_field(
    meta::field_type const& type,
    accessor::record_ref target,
    std::size_t target_offset,
    std::size_t target_nullity_offset,
    data::any const& source,
    memory::paged_memory_resource* resource = nullptr
);

/**
 * @brief copy non-nullable field data on source record to data::any value container
 * @param type type of the record field being read
 * @param source source record reference
 * @param source_offset byte offset of the source field in the source record
 * @param result [OUT] argument to receive the result
 * @param resource memory resource optionally used to allocate varlen data on the target. Pass nullptr if no new
 * allocation is needed and the target still references the same varlen buffer as source.
 */
void copy_field_as_any(
    meta::field_type const& type,
    accessor::record_ref source,
    std::size_t source_offset,
    data::any& result,
    memory::paged_memory_resource* resource = nullptr
);

/**
 * @brief copy nullable field data on source record to data::any value container
 * @param type type of the record field being read
 * @param source source record reference
 * @param source_offset byte offset of the source field in the source record
 * @param source_nullity_offset bit offset of the nullity flags in the source record
 * @param result [OUT] argument to receive the result
 * @param resource memory resource optionally used to allocate varlen data on the target. Pass nullptr if no new
 * allocation is needed and the target still references the same varlen buffer as source.
 */
void copy_nullable_field_as_any(
    meta::field_type const& type,
    accessor::record_ref source,
    std::size_t source_offset,
    std::size_t source_nullity_offset,
    data::any& result,
    memory::paged_memory_resource* resource = nullptr
);

}  // namespace jogasaki::utils
