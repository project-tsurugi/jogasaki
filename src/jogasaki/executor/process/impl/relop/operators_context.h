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

#include <unordered_map>

#include <jogasaki/executor/process/impl/relop/operator_base.h>
#include <jogasaki/executor/process/impl/relop/context_base.h>

namespace jogasaki::executor::process::impl::relop {

/**
 * @brief relational operators container
 */
class operators_context {
public:
    using contexts_type = std::unordered_map<relop::operator_base const*, std::unique_ptr<relop::context_base>>;

    operators_context() = default;

    explicit operators_context(
        contexts_type operators
    ) :
        contexts_(std::move(operators))
    {}

    [[nodiscard]] contexts_type& contexts() noexcept {
        return contexts_;
    }

private:
    contexts_type contexts_{};
};

}

