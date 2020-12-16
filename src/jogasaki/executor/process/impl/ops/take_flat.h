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

#include <vector>

#include <takatori/util/downcast.h>
#include <takatori/util/sequence_view.h>
#include <takatori/relation/step/take_flat.h>

#include <jogasaki/executor/reader_container.h>
#include <jogasaki/meta/variable_order.h>
#include "operator_base.h"
#include "take_flat_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using takatori::util::unsafe_downcast;

namespace details {

struct cache_align take_flat_field {
    meta::field_type type_{};
    std::size_t source_offset_{};
    std::size_t target_offset_{};
    std::size_t source_nullity_offset_{};
    std::size_t target_nullity_offset_{};
    bool nullable_{};
};

}

/**
 * @brief take_flat operator
 */
class take_flat : public record_operator {
public:
    friend class take_flat_context;

    using column = takatori::relation::step::take_flat::column;

    /**
     * @brief create empty object
     */
    take_flat() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param order the exchange columns ordering information that assigns the field index of the input record. The index
     * can be used with record_meta to get field metadata.
     * @param meta the record metadata of the record. This information is typically provided by the upstream exchange.
     * @param columns mapping information between exchange columns and variables
     * @param reader_index the index that identifies the reader in the task context. This corresponds to the input port
     * number that the input exchange is connected.
     * @param downstream downstream operator that should be invoked with the output from this operation
     */
    take_flat(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        meta::variable_order const& order,
        maybe_shared_ptr<meta::record_meta> meta,
        takatori::util::sequence_view<column const> columns,
        std::size_t reader_index,
        std::unique_ptr<operator_base> downstream = nullptr
    );

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     */
    void process_record(abstract::task_context* context) override;

    /**
     * @brief process record with context object
     * @details process record, fill variables, and invoke downstream
     * @param ctx operator context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     */
    void operator()(take_flat_context& ctx, abstract::task_context* context = nullptr);

    [[nodiscard]] operator_kind kind() const noexcept override;

    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept;

    void finish(abstract::task_context*) override;
private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    std::vector<details::take_flat_field> fields_{};
    std::size_t reader_index_{};
    std::unique_ptr<operator_base> downstream_{};

    [[nodiscard]] std::vector<details::take_flat_field> create_fields(
        maybe_shared_ptr<meta::record_meta> const& meta,
        meta::variable_order const& order,
        takatori::util::sequence_view<column const> columns
    );
};

}


