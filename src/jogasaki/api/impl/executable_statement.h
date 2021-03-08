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

#include <jogasaki/api/executable_statement.h>
#include <jogasaki/plan/executable_statement.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>

namespace jogasaki::api::impl {

/**
 * @brief executable statement implementation
 * @details this object holds plan::executable_statement together with memory resource, that is
 * used for variable length data during compilation.
 */
class executable_statement : public api::executable_statement {
public:
    executable_statement() = default;

    executable_statement(
        std::shared_ptr<plan::executable_statement> body,
        std::shared_ptr<memory::lifo_paged_memory_resource> resource
    );

    /**
     * @brief accessor to the wrapped object
     * @return plan::executable_statement holding compiled result and jogasaki artifacts
     */
    [[nodiscard]] std::shared_ptr<plan::executable_statement> const& body() const noexcept;

    /**
     * @brief accessor to the compile-time memory resource
     * @return resource used in the compile-time processing
     */
    [[nodiscard]] std::shared_ptr<memory::lifo_paged_memory_resource> const& resource() const noexcept;

private:
    std::shared_ptr<plan::executable_statement> body_{};
    std::shared_ptr<memory::lifo_paged_memory_resource> resource_{};
};

}
