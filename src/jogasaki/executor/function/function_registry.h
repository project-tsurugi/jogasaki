/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <vector>

#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/function/configurable_provider.h>

#include <jogasaki/executor/function/aggregate_function_repository.h>
#include <jogasaki/executor/function/incremental/aggregate_function_repository.h>
#include <jogasaki/executor/function/scalar_function_repository.h>
#include <jogasaki/executor/function/table_valued_function_repository.h>
#include <jogasaki/udf/plugin_loader.h>

namespace jogasaki { class configuration; }

namespace jogasaki::executor::function {

/** @brief database-owned declarations, implementations, and plugin resources */
class function_registry {
public:
    function_registry() = default;
    ~function_registry() = default;
    function_registry(function_registry const&) = delete;
    function_registry& operator=(function_registry const&) = delete;
    function_registry(function_registry&&) = delete;
    function_registry& operator=(function_registry&&) = delete;

    void initialize(configuration const& cfg);
    /** @brief release all function registrations after execution has stopped */
    void clear();

    [[nodiscard]] scalar_function_repository& scalar_functions() noexcept { return scalar_functions_; }
    [[nodiscard]] table_valued_function_repository& table_functions() noexcept { return table_functions_; }
    [[nodiscard]] aggregate_function_repository& aggregate_functions() noexcept { return aggregate_functions_; }
    [[nodiscard]] incremental::aggregate_function_repository& incremental_functions() noexcept { return incremental_functions_; }
    [[nodiscard]] std::shared_ptr<yugawara::function::configurable_provider> const& regular_provider() const noexcept { return regular_provider_; }
    [[nodiscard]] std::shared_ptr<yugawara::aggregate::configurable_provider> const& aggregate_provider() const noexcept { return aggregate_provider_; }
    void regular_provider(std::shared_ptr<yugawara::function::configurable_provider> value);

private:
    // Registrations are destroyed before plugin resources (reverse member order).
    std::unique_ptr<plugin::udf::plugin_loader> loader_{};
    std::vector<plugin::udf::plugin_entry> plugins_{};
    std::shared_ptr<yugawara::function::configurable_provider> regular_provider_{
        std::make_shared<yugawara::function::configurable_provider>()};
    std::shared_ptr<yugawara::aggregate::configurable_provider> aggregate_provider_{
        std::make_shared<yugawara::aggregate::configurable_provider>()};
    scalar_function_repository scalar_functions_{};
    table_valued_function_repository table_functions_{};
    aggregate_function_repository aggregate_functions_{};
    incremental::aggregate_function_repository incremental_functions_{};
};
}
