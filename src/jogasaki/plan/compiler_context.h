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

#include <takatori/util/object_creator.h>
#include <yugawara/compiler_result.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/variable/configurable_provider.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/aggregate/configurable_provider.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::plan {

/**
 * @brief vehicle of objects generated during sql compilation
 */
class cache_align compiler_context {
public:

    void compiler_result(yugawara::compiler_result compiler_result) noexcept {
        statement_ = compiler_result.release_statement();
        compiled_info_ = std::move(compiler_result.info());
    }

    [[nodiscard]] ::takatori::statement::statement& statement() noexcept {
        return *statement_;
    }

    void compiled_info(yugawara::compiled_info compiled_info) noexcept {
        compiled_info_ = std::move(compiled_info);
    }

    void statement(std::unique_ptr<::takatori::statement::statement> statement) noexcept {
        takatori::util::object_creator creator{};
        statement_ = creator.wrap_unique(statement.release());
    }

    [[nodiscard]] yugawara::compiled_info& compiled_info() noexcept {
        return compiled_info_;
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

    void variable_provider(std::shared_ptr<::yugawara::variable::configurable_provider> variable_provider) noexcept {
        variable_provider_ = std::move(variable_provider);
    }

    [[nodiscard]] std::shared_ptr<::yugawara::variable::configurable_provider> const& variable_provider() const {
        return variable_provider_;
    }

    void function_provider(std::shared_ptr<::yugawara::function::configurable_provider> function_provider) noexcept {
        function_provider_ = std::move(function_provider);
    }

    [[nodiscard]] std::shared_ptr<::yugawara::function::configurable_provider> const& function_provider() const {
        return function_provider_;
    }

    void aggregate_provider(std::shared_ptr<::yugawara::aggregate::configurable_provider> aggregate_provider) noexcept {
        aggregate_provider_ = std::move(aggregate_provider);
    }

    [[nodiscard]] std::shared_ptr<::yugawara::aggregate::configurable_provider> const& aggregate_provider() const {
        return aggregate_provider_;
    }
private:
    takatori::util::unique_object_ptr<::takatori::statement::statement> statement_{};
    yugawara::compiled_info compiled_info_{};
    std::shared_ptr<model::graph> step_graph_{};
    std::shared_ptr<::yugawara::storage::configurable_provider> storage_provider_{};
    std::shared_ptr<::yugawara::variable::configurable_provider> variable_provider_{};
    std::shared_ptr<::yugawara::function::configurable_provider> function_provider_{};
    std::shared_ptr<::yugawara::aggregate::configurable_provider> aggregate_provider_{};
};

}
