/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <takatori/util/sequence_view.h>
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
#include <jogasaki/executor/wrt/write_field.h>
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

using takatori::util::sequence_view;

static constexpr std::size_t npos = static_cast<std::size_t>(-1);

status next_sequence_value(request_context& ctx, sequence_definition_id def_id, sequence_value& out);

status fill_default_value(
    wrt::write_field const& f,
    request_context& ctx,
    memory::lifo_paged_memory_resource& resource,
    data::small_record_store& out
);

void create_generated_field(
    std::vector<wrt::write_field>& ret,
    std::size_t index,
    yugawara::storage::column_value const& dv,
    takatori::type::data const& type,
    bool nullable,
    kvs::coding_spec spec,
    std::size_t offset,
    std::size_t nullity_offset,
    memory::lifo_paged_memory_resource* resource
);

std::vector<wrt::write_field> create_fields(
    yugawara::storage::index const& idx,
    sequence_view<takatori::relation::details::mapping_element const> columns,
    maybe_shared_ptr<meta::record_meta> key_meta,  //NOLINT(performance-unnecessary-value-param)
    maybe_shared_ptr<meta::record_meta> value_meta,  //NOLINT(performance-unnecessary-value-param)
    bool key,
    memory::lifo_paged_memory_resource* resource
);

std::vector<wrt::write_field> create_fields(
    yugawara::storage::index const& idx,
    sequence_view<takatori::descriptor::variable const> columns,
    maybe_shared_ptr<meta::record_meta> key_meta,  //NOLINT(performance-unnecessary-value-param)
    maybe_shared_ptr<meta::record_meta> value_meta,  //NOLINT(performance-unnecessary-value-param)
    bool key,
    memory::lifo_paged_memory_resource* resource
);

primary_target create_primary_target(
    std::string_view storage_name,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta,
    std::vector<wrt::write_field> const& key_fields,
    std::vector<wrt::write_field> const& value_fields
);

std::vector<secondary_target> create_secondary_targets(
    yugawara::storage::index const& idx,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta
);

}  // namespace jogasaki::executor::wrt
