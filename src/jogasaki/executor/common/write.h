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

#include <takatori/statement/write.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/compiled_info.h>
#include <yugawara/storage/index.h>

#include <jogasaki/common_types.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/common/step.h>
#include <jogasaki/executor/process/impl/ops/default_value_kind.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
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

namespace jogasaki::executor::common {

using jogasaki::executor::process::impl::ops::write_kind;
using yugawara::compiled_info;
using primary_target = jogasaki::index::primary_target;
using primary_context = jogasaki::index::primary_context;
using secondary_target = jogasaki::index::secondary_target;
using secondary_context = jogasaki::index::secondary_context;

namespace details {

/**
 * @brief field info. for write
 */
struct write_field : process::impl::ops::default_value_property {
    write_field(
        std::size_t index,
        meta::field_type type,
        kvs::coding_spec spec,
        bool nullable,
        std::size_t offset,
        std::size_t nullity_offset
    ) :
        index_(index),
        type_(std::move(type)),
        spec_(spec),
        nullable_(nullable),
        offset_(offset),
        nullity_offset_(nullity_offset) {}

    write_field(
        std::size_t index,
        meta::field_type type,
        kvs::coding_spec spec,
        bool nullable,
        std::size_t offset,
        std::size_t nullity_offset,
        process::impl::ops::default_value_kind kind,
        data::any immediate_value,
        sequence_definition_id def_id
    ) :
        default_value_property(
            kind,
            immediate_value,
            def_id
        ),
        index_(index),
        type_(std::move(type)),
        spec_(spec),
        nullable_(nullable),
        offset_(offset),
        nullity_offset_(nullity_offset) {}

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
};

}  // namespace details

class write_context {
public:
    write_context(
        request_context& context,
        std::string_view storage_name,
        maybe_shared_ptr<meta::record_meta> key_meta,
        maybe_shared_ptr<meta::record_meta> value_meta,
        std::vector<secondary_target> const& secondaries,
        kvs::database& db,
        memory::lifo_paged_memory_resource* resource
    );

    request_context* request_context_{};  //NOLINT
    primary_context primary_context_{};  //NOLINT
    std::vector<secondary_context> secondary_contexts_{};  //NOLINT
    data::small_record_store key_store_{};  //NOLINT
    data::small_record_store value_store_{};  //NOLINT
};

/**
 * @brief write statement (to execute Insert)
 */
class write : public model::statement {
public:
    using column = takatori::statement::write::column;
    using tuple = takatori::statement::write::tuple;

    /**
     * @brief create empty object
     */
    write() = default;

    /**
     * @brief create new object
     */
    write(
        write_kind kind,
        yugawara::storage::index const& idx,
        takatori::statement::write const& wrt,
        memory::lifo_paged_memory_resource& resource,
        compiled_info info,
        executor::process::impl::variable_table const* host_variables
    );

    [[nodiscard]] model::statement_kind kind() const noexcept override;

    bool operator()(request_context& context);

    bool process(request_context& context);

private:
    write_kind kind_{};
    yugawara::storage::index const* idx_{};
    takatori::statement::write const* wrt_{};
    memory::lifo_paged_memory_resource* resource_{};
    compiled_info info_{};
    executor::process::impl::variable_table const* host_variables_{};
    maybe_shared_ptr<meta::record_meta> key_meta_{};
    maybe_shared_ptr<meta::record_meta> value_meta_{};
    std::vector<details::write_field> key_fields_{};
    std::vector<details::write_field> value_fields_{};
    primary_target primary_{};
    std::vector<secondary_target> secondaries_{};

    bool put_primary(write_context& wctx, bool& skip_error, std::string_view& encoded_primary_key);
    bool try_insert_primary(write_context& wctx, bool& primary_already_exists, std::string_view& encoded_primary_key);
    bool put_secondaries(write_context& wctx, std::string_view encoded_primary_key);
    bool update_secondaries_before_upsert(
        write_context& wctx,
        std::string_view encoded_primary_key,
        bool primary_already_exists
    );
};

}  // namespace jogasaki::executor::common
