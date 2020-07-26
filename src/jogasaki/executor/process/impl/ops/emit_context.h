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

#include <jogasaki/data/small_record_store.h>
#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief emit context
 */
class emit_context : public context_base {
public:
    friend class emit;
    /**
     * @brief create empty object
     */
    emit_context() = default;

    /**
     * @brief create new object
     */
    explicit emit_context(
        class abstract::task_context* ctx,
        std::shared_ptr<meta::record_meta> meta,
        block_scope& variables
    ) : context_base(ctx, variables),
        store_(std::move(meta))
    {}

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::emit;
    }

    void release() override {
        if(writer_) {
            writer_->release();
            writer_ = nullptr;
        }
    }
private:
    data::small_record_store store_{};
    record_writer* writer_{};
};

}


