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

namespace jogasaki::executor::common {

using jogasaki::executor::process::impl::ops::write_kind;
using yugawara::compiled_info;
using primary_target = jogasaki::index::primary_target;
using primary_context = jogasaki::index::primary_context;
using secondary_target = jogasaki::index::secondary_target;
using secondary_context = jogasaki::index::secondary_context;

/**
 * @brief write statement (to execute Insert)
 */
class write_statement : public model::statement {
public:
    using column = takatori::statement::write::column;
    using tuple = takatori::statement::write::tuple;

    /**
     * @brief create empty object
     */
    write_statement() = default;

    /**
     * @brief create new object
     */
    write_statement(
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
    std::vector<wrt::write_field> key_fields_{};
    std::vector<wrt::write_field> value_fields_{};
    std::shared_ptr<wrt::insert_new_record> entity_{};
};

}  // namespace jogasaki::executor::common
