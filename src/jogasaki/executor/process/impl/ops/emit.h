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

#include <takatori/util/sequence_view.h>
#include <takatori/util/object_creator.h>
#include <takatori/relation/emit.h>
#include <takatori/descriptor/variable.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/executor/process/impl/block_scope.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/validation.h>
#include "operator_base.h"
#include <jogasaki/executor/process/impl/ops/context_helper.h>
#include "emit_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

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
        takatori::util::sequence_view<column const> columns
    );

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     */
    void process_record(abstract::task_context* context) override;

    /**
     * @brief process record with context object
     * @details emit the record and copy result to client buffer
     * @param ctx operator context object for the execution
     */
    void operator()(emit_context& ctx);

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::emit;
    }

    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept;

    void external_writer_index(std::size_t index) noexcept;

    void finish(abstract::task_context* context) override {
        BOOST_ASSERT(context != nullptr);  //NOLINT
        context_helper ctx{*context};
        auto* p = find_context<emit_context>(index(), ctx.contexts());
        if (p && p->writer_) {
            p->writer_->flush();
        }
    }
private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    std::vector<details::emit_field> fields_{};
    std::size_t external_writer_index_{};

    [[nodiscard]] std::shared_ptr<meta::record_meta> create_meta(
        processor_info const& info,
        takatori::util::sequence_view<column const> columns
    );

    [[nodiscard]] std::vector<details::emit_field> create_fields(
        maybe_shared_ptr<meta::record_meta> const& meta,
        takatori::util::sequence_view<column const> columns
    );
};

}


