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

#include <jogasaki/model/statement.h>
#include <jogasaki/request_context.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/model/statement_kind.h>

namespace jogasaki::executor::common {

/**
 * @brief graph common implementation
 */
class execute : public model::statement {
public:
    /**
     * @brief create empty object
     */
    execute() = default;

    /**
     * @brief create new execute object from graph
     * @param graph the graph to execute
     */
    explicit execute(std::shared_ptr<common::graph> graph) noexcept;

    /**
     * @brief request context setter
     * @param context the request context to set
     */
    void context(request_context& context);

    /**
     * @brief accessor to the request context
     */
    [[nodiscard]] request_context* context() const noexcept;

    /**
     * @brief accessor to the statement kind
     * @return the statement kind
     */
    [[nodiscard]] model::statement_kind kind() const noexcept override;

    /**
     * @brief accessor to the owned graph
     * @return the graph
     */
    [[nodiscard]] common::graph& operators() const noexcept;

private:
    std::shared_ptr<common::graph> operators_{};
    request_context* context_{};
};

}
