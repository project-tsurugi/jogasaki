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

#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/external_writer.h>
#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief emit context
 */
class emit_context : public context_base {
public:
    using memory_resource = memory::lifo_paged_memory_resource;

    friend class emit;
    /**
     * @brief create empty object
     */
    emit_context() = default;

    /**
     * @brief create new object
     */
    emit_context(
        class abstract::task_context* ctx,
        variable_table& variables,
        maybe_shared_ptr<meta::record_meta> meta,
        memory_resource* resource = nullptr,
        memory_resource* varlen_resource = nullptr
    );

    // for test
    [[nodiscard]] data::small_record_store& store() noexcept;

    [[nodiscard]] operator_kind kind() const noexcept override;

    void release() override;

private:
    data::small_record_store buffer_{};
    external_writer* writer_{};
};

}


