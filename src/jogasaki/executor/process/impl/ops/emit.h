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
#include <memory>
#include <vector>

#include <takatori/descriptor/variable.h>
#include <takatori/relation/emit.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/interference_size.h>

#include "emit_context.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;
using takatori::util::sequence_view;

namespace details {

struct cache_align emit_field {
    meta::field_type type_{};
    std::size_t source_offset_{};
    std::size_t target_offset_{};
    std::size_t source_nullity_offset_{};
    std::size_t target_nullity_offset_{};
    bool nullable_{};
};

}

/**
 * @brief emit operator
 */
class emit : public record_operator {
public:
    friend class emit_context;

    using column = takatori::relation::emit::column;

    /**
     * @brief create empty object
     */
    emit() = default;

    /**
     * @brief create new object
     */
    emit(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        sequence_view<column const> columns
    );

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     * @return status of the operation
     */
    operation_status process_record(abstract::task_context* context) override;

    /**
     * @brief process record with context object
     * @details emit the record and copy result to client buffer
     * @param ctx operator context object for the execution
     * @return status of the operation
     */
    operation_status operator()(emit_context& ctx);

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @brief access to the record metadata
     */
    [[nodiscard]] maybe_shared_ptr<meta::external_record_meta> const& meta() const noexcept;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context* context) override;

    static std::shared_ptr<meta::external_record_meta> create_meta(
        yugawara::compiled_info const& info,
        sequence_view<const column> columns
    );
private:
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    std::vector<details::emit_field> fields_{};

    [[nodiscard]] std::vector<details::emit_field> create_fields(
        maybe_shared_ptr<meta::record_meta> const& meta,
        sequence_view<column const> columns
    );
};

}


