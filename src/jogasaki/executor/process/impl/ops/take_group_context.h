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
#include <jogasaki/executor/group_reader.h>
#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief take_group context
 */
class take_group_context : public context_base {
public:
    friend class take_group;
    /**
     * @brief create empty object
     */
    take_group_context() = default;

    /**
     * @brief create new object
     */
    explicit take_group_context(
        class abstract::task_context* ctx,
        block_scope& variables,
        memory_resource* resource
    ) : context_base(ctx, variables, resource)
    {}

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::take_group;
    }

    void release() override {
        if(reader_) {
            reader_->release();
            reader_ = nullptr;
        }
    }
private:
    group_reader* reader_{};
};

}


