/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/compiler_result.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/variable/configurable_provider.h>

#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/plan/compile_option.h>
#include <jogasaki/plan/executable_statement.h>
#include <jogasaki/plan/prepared_statement.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::plan {

/**
 * @brief vehicle of objects generated during sql compilation
 * @details This object lives during compilation time, i.e. when compilation completes, the context retires and will be
 * deleted.
 */
class cache_align compiler_context {
public:

    void prepared_statement(std::shared_ptr<prepared_statement> statement) noexcept {
        prepared_statement_ = std::move(statement);
    }

    [[nodiscard]] std::shared_ptr<class prepared_statement> const& prepared_statement() noexcept {
        return prepared_statement_;
    }

    void executable_statement(std::shared_ptr<class executable_statement> statement) noexcept {
        executable_statement_ = std::move(statement);
    }

    [[nodiscard]] std::shared_ptr<class executable_statement> const& executable_statement() noexcept {
        return executable_statement_;
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

    void resource(std::shared_ptr<memory::lifo_paged_memory_resource> resource) noexcept {
        resource_ = std::move(resource);
    }

    [[nodiscard]] memory::lifo_paged_memory_resource* resource() const noexcept {
        return resource_.get();
    }

    void sql_text(std::shared_ptr<std::string> sql) noexcept {
        sql_text_ = std::move(sql);
    }

    [[nodiscard]] std::string_view sql_text() const {
        if(! sql_text_) return {};
        return *sql_text_;
    }

    [[nodiscard]] std::shared_ptr<std::string> sql_text_shared() const {
        return sql_text_;
    }

    void error_info(std::shared_ptr<error::error_info> info) noexcept {
        error_info_ = std::move(info);
    }

    [[nodiscard]] std::shared_ptr<error::error_info> const& error_info() const noexcept {
        return error_info_;
    }

    void option(compile_option option) noexcept {
        option_ = option;
    }

    [[nodiscard]] compile_option const& option() const noexcept {
        return option_;
    }

private:
    std::shared_ptr<class prepared_statement> prepared_statement_{};
    std::shared_ptr<class executable_statement> executable_statement_{};
    std::shared_ptr<::yugawara::storage::configurable_provider> storage_provider_{};
    std::shared_ptr<::yugawara::variable::configurable_provider> variable_provider_{};
    std::shared_ptr<::yugawara::function::configurable_provider> function_provider_{};
    std::shared_ptr<::yugawara::aggregate::configurable_provider> aggregate_provider_{};
    std::shared_ptr<memory::lifo_paged_memory_resource> resource_{};
    std::shared_ptr<std::string> sql_text_{};
    std::shared_ptr<error::error_info> error_info_{};
    compile_option option_{};

};

}
