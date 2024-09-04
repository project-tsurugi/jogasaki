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
#include <string_view>
#include <utility>
#include <vector>

#include <yugawara/storage/index.h>

#include <jogasaki/common_types.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/common/step.h>
#include <jogasaki/executor/process/impl/ops/default_value_kind.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/wrt/insert_new_record.h>
#include <jogasaki/index/primary_context.h>
#include <jogasaki/index/primary_target.h>
#include <jogasaki/index/secondary_context.h>
#include <jogasaki/index/secondary_target.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/model/statement_kind.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::wrt {

/**
 * @brief field info. for write
 */
struct write_field : process::impl::ops::default_value_property {
    write_field(
        std::size_t index,
        takatori::type::data const& target_type,
        kvs::coding_spec spec,
        bool nullable,
        std::size_t offset,
        std::size_t nullity_offset
    ) :
        index_(index),
        type_(utils::type_for(target_type)),
        spec_(spec),
        nullable_(nullable),
        offset_(offset),
        nullity_offset_(nullity_offset),
        target_type_(std::addressof(target_type))
    {}

    write_field(
        std::size_t index,
        takatori::type::data const& target_type,
        kvs::coding_spec spec,
        bool nullable,
        std::size_t offset,
        std::size_t nullity_offset,
        process::impl::ops::default_value_kind kind,
        data::any immediate_value,
        sequence_definition_id def_id,
        yugawara::function::configurable_provider const* functions = nullptr
    ) :
        default_value_property(
            kind,
            immediate_value,
            def_id,
            functions
        ),
        index_(index),
        type_(utils::type_for(target_type)),
        spec_(spec),
        nullable_(nullable),
        offset_(offset),
        nullity_offset_(nullity_offset),
        target_type_(std::addressof(target_type))
    {}

    //@brief value position in the tuple. npos if values clause doesn't contain one for this field.
    std::size_t index_{};  //NOLINT
    //@brief field type
    meta::field_type type_{};  //NOLINT
    //@brief coding spec
    kvs::coding_spec spec_{};  //NOLINT
    //@brief if the field is nullable
    bool nullable_{};  //NOLINT
    //@brief value offset
    std::size_t offset_{};  //NOLINT
    //@brief nullity bit offset
    std::size_t nullity_offset_{};  //NOLINT
    //@brief original target type
    takatori::type::data const* target_type_{};  //NOLINT
};

}  // namespace jogasaki::executor::wrt
