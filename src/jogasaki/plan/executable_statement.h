/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <memory>
#include <string>
#include <string_view>

#include <takatori/statement/statement.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::plan {

using takatori::util::maybe_shared_ptr;
using executor::process::impl::variable_table;
using executor::process::impl::variable_table_info;

/**
 * @brief executable statement
 */
class cache_align executable_statement {
public:

    /**
     * @brief create empty object
     */
    executable_statement() = default;

    /**
     * @brief create new object
     * @param statement the statement from compiler
     * @param compiled_info the compiler result
     * @param operators jogasaki graph or execute statement
     */
    executable_statement(
        maybe_shared_ptr<::takatori::statement::statement> statement,
        yugawara::compiled_info compiled_info,
        maybe_shared_ptr<model::statement> operators,
        std::shared_ptr<variable_table_info> host_variable_info,
        std::shared_ptr<variable_table> host_variables,
        std::shared_ptr<mirror_container> mirrors,
        std::shared_ptr<std::string> sql_text
    ) noexcept;

    [[nodiscard]] maybe_shared_ptr<model::statement> const& operators() const noexcept;

    [[nodiscard]] bool is_execute() const noexcept;

    [[nodiscard]] bool is_ddl() const noexcept;

    [[nodiscard]] bool is_empty() const noexcept;

    [[nodiscard]] maybe_shared_ptr<::takatori::statement::statement> const& statement() const noexcept;

    [[nodiscard]] yugawara::compiled_info const& compiled_info() const noexcept;

    [[nodiscard]] std::shared_ptr<variable_table> const& host_variables() const noexcept;

    [[nodiscard]] std::shared_ptr<variable_table_info> const& host_variable_info() const noexcept;

    [[nodiscard]] std::shared_ptr<mirror_container> const& mirrors() const noexcept;

    [[nodiscard]] std::string_view sql_text() const noexcept;

    [[nodiscard]] std::shared_ptr<std::string> const& sql_text_shared() const noexcept;

    /**
     * @brief accessor to the mirror_container
     * @return the mirror container
     */
    [[nodiscard]] std::shared_ptr<mirror_container> get_mirrors() const noexcept ;
private:
    maybe_shared_ptr<::takatori::statement::statement> statement_{};
    yugawara::compiled_info compiled_info_{};
    maybe_shared_ptr<model::statement> operators_{};
    std::shared_ptr<variable_table_info> host_variable_info_{};
    std::shared_ptr<variable_table> host_variables_{};
    std::shared_ptr<mirror_container> mirrors_{};
    std::shared_ptr<std::string> sql_text_{};
};

} // namespace jogasaki::plan
