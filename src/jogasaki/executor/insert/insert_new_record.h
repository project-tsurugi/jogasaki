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

#include <takatori/type/data.h>
#include <takatori/util/maybe_shared_ptr.h>
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
#include <jogasaki/request_context.h>

namespace jogasaki::executor::insert {

using jogasaki::executor::process::impl::ops::write_kind;
using primary_target = jogasaki::index::primary_target;
using primary_context = jogasaki::index::primary_context;
using secondary_target = jogasaki::index::secondary_target;
using secondary_context = jogasaki::index::secondary_context;

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
    memory::lifo_paged_memory_resource* resource_{};
};

/**
 * @brief implementation of core logic to insert new record (common for Write operator and statement)
 */
class insert_new_record {
public:
    /**
     * @brief create empty object
     */
    insert_new_record() = default;

    /**
     * @brief create new object
     */
    insert_new_record(
        write_kind kind,
        yugawara::storage::index const& idx,
        primary_target primary,
        std::vector<secondary_target> secondaries
    );

    bool process_record(request_context& context, write_context& wctx);

    std::vector<secondary_target>& secondaries() noexcept {
        return secondaries_;
    }
private:
    write_kind kind_{};
    yugawara::storage::index const* idx_{};
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
