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
#include <jogasaki/executor/process/abstract/work_context.h>
#include <jogasaki/executor/process/impl/relop/operators_context.h>
#include <jogasaki/executor/process/impl/block_variables.h>

namespace jogasaki::executor::process::impl {

/**
 * @brief processor working context implementation for production
 */
class work_context : public process::abstract::work_context {
public:
    using variables_type = std::vector<block_variables>;

    work_context() = default;

    [[nodiscard]] relop::operators_context& contexts() noexcept {
        return contexts_;
    }
    [[nodiscard]] variables_type& variables() noexcept {
        return variables_;
    }

private:
    relop::operators_context contexts_{};
    variables_type variables_{};
};

}


