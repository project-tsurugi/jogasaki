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

#include <cstddef>
#include <functional>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/model/graph.h>

#include <yugawara/compiler_result.h>
#include <yugawara/storage/configurable_provider.h>

namespace jogasaki::plan {

class compiler_context {
public:

    void compiler_result(yugawara::compiler_result compiler_result) noexcept {
        compiler_result_ = std::move(compiler_result);
    }

    [[nodiscard]] yugawara::compiler_result const& compiler_result() const noexcept {
        return compiler_result_;
    }

    void step_graph(std::shared_ptr<model::graph> step_graph) noexcept {
        step_graph_ = std::move(step_graph);
    }

    [[nodiscard]] model::graph* step_graph() const noexcept {
        return step_graph_.get();
    }

    void storage_provider(std::shared_ptr<::yugawara::storage::configurable_provider> storage_provider) noexcept {
        storage_provider_ = std::move(storage_provider);
    }

    [[nodiscard]] std::shared_ptr<::yugawara::storage::configurable_provider> const& storage_provider() const {
        return storage_provider_;
    }

private:
    yugawara::compiler_result compiler_result_{};
    std::shared_ptr<model::graph> step_graph_{};
    std::shared_ptr<::yugawara::storage::configurable_provider> storage_provider_{};
};

}
