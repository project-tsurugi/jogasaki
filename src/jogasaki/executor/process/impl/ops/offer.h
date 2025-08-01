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
#include <vector>

#include <takatori/relation/step/offer.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>

#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/utils/interference_size.h>

#include "offer_context.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::sequence_view;

namespace details {

struct cache_align offer_field {
    meta::field_type type_{};
    meta::field_type source_ftype_{};
    std::size_t source_offset_{};
    std::size_t target_offset_{};
    std::size_t source_nullity_offset_{};
    std::size_t target_nullity_offset_{};
    bool nullable_{};
    takatori::type::data const* source_type_{};
    takatori::type::data const* target_type_{};
    bool requires_conversion_{};
};

}

/**
 * @brief offer operator
 */
class offer : public record_operator {
public:
    friend class offer_context;

    using column = takatori::relation::step::offer::column;

    /**
     * @brief create empty object
     */
    offer() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param order the exchange columns ordering information that assigns the field index of the output record. The index
     * can be used with record_meta to get field metadata.
     * @param meta the record metadata of the output record. This information is typically provided by the downstream exchange.
     * @param columns mapping information between variables and exchange columns
     * @param writer_index the index that identifies the writer in the task context. This corresponds to the output port
     * number that the output exchange is connected.
     */
    offer(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        meta::variable_order const& order,
        maybe_shared_ptr<meta::record_meta> meta,
        sequence_view<column const> columns,
        std::size_t writer_index
    );

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     * @return status of the operation
     */
    operation_status process_record(abstract::task_context* context) override;

    /**
     * @brief process record with context object
     * @param ctx operator context object for the execution
     * @return status of the operation
     */
    operation_status operator()(offer_context& ctx);

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @see accessor to record metadata
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context* context) override;

private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    std::vector<details::offer_field> fields_{};
    std::size_t writer_index_{};

    [[nodiscard]] std::vector<details::offer_field> create_fields(
        maybe_shared_ptr<meta::record_meta> const& meta,
        meta::variable_order const& order,
        sequence_view<column const> columns,
        processor_info const& info
    );
};

}  // namespace jogasaki::executor::process::impl::ops
