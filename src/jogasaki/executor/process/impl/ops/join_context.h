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

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;

/**
 * @brief join context
 */
class join_context : public context_base {
public:
    template <class Iterator>
    friend class join;

    /**
     * @brief create empty object
     */
    join_context() = default;

    /**
     * @brief create new object
     */
    join_context(
        class abstract::task_context* ctx,
        variable_table& variables,
        memory_resource* resource = nullptr,
        memory_resource* varlen_resource = nullptr
    ) :
        context_base(ctx, variables, resource, varlen_resource)
    {}

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::join;
    }

    void release() override {
        //no-op
    }

private:

};

}


