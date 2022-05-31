/*
 * Copyright 2018-2020 tsurugi project.
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

#include <memory>
#include <queue>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>

#include <takatori/relation/step/take_cogroup.h>
#include <takatori/relation/step/take_group.h>

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/group_reader.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/utils/iterator_pair.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include "take_cogroup_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;

class group_element {
public:
    using column = takatori::relation::step::take_group::column;

    group_element(
        meta::variable_order const& order,
        maybe_shared_ptr<meta::group_meta> meta,
        sequence_view<column const> columns,
        std::size_t reader_index,
        variable_table_info const& block_info
    );

    meta::variable_order const* order_{}; //NOLINT
    maybe_shared_ptr<meta::group_meta> meta_{}; //NOLINT
    std::size_t reader_index_{}; //NOLINT
    std::vector<group_field> fields_{}; //NOLINT
    meta::record_meta const* key_meta_{}; //NOLINT

    [[nodiscard]] std::vector<group_field> create_fields(
        maybe_shared_ptr<meta::group_meta> const& meta,
        meta::variable_order const& order,
        takatori::util::sequence_view<column const> columns,
        variable_table_info const& block_info
    );
};

class take_cogroup : public record_operator {
public:
    using iterator_pair = utils::iterator_pair<details::group_input::iterator>;
    using queue_type = take_cogroup_context::queue_type;
    using input_index = take_cogroup_context::input_index;

    take_cogroup(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        std::vector<group_element> groups,
        std::unique_ptr<operator_base> downstream = nullptr
    );

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     * @return status of the operation
     */
    operation_status process_record(abstract::task_context* context) override;

    /**
     * @brief process record with context object
     * @details process record, fill variables, and invoke downstream
     * @param ctx operator context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     * @return status of the operation
     */
    operation_status operator()(take_cogroup_context& ctx, abstract::task_context* context = nullptr);

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context*) override;

private:
    std::vector<group_element> groups_{};
    std::unique_ptr<operator_base> downstream_{};
    std::vector<sequence_view<group_field>> fields_{};

    void create_readers(take_cogroup_context& ctx);
};

}
