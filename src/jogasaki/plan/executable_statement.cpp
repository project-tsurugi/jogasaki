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
#include "executable_statement.h"

#include <type_traits>
#include <utility>

#include <takatori/statement/statement_kind.h>

#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/plan/mirror_container.h>

namespace jogasaki::plan {

bool executable_statement::is_execute() const noexcept {
    return statement_ && statement_->kind() == takatori::statement::statement_kind::execute;
}

bool executable_statement::is_empty() const noexcept {
    return statement_ && statement_->kind() == takatori::statement::statement_kind::empty;
}

executable_statement::executable_statement(
    maybe_shared_ptr<::takatori::statement::statement> statement,
    yugawara::compiled_info compiled_info,
    maybe_shared_ptr<model::statement> operators,
    std::shared_ptr<variable_table_info> host_variable_info,
    std::shared_ptr<variable_table> host_variables,
    std::shared_ptr<mirror_container> mirrors,
    std::shared_ptr<std::string> sql_text
) noexcept:
    statement_(std::move(statement)),
    compiled_info_(std::move(compiled_info)),
    operators_(std::move(operators)),
    host_variable_info_(std::move(host_variable_info)),
    host_variables_(std::move(host_variables)),
    mirrors_(std::move(mirrors)),
    sql_text_(std::move(sql_text))
{}

maybe_shared_ptr<model::statement> const& executable_statement::operators() const noexcept {
    return operators_;
}

maybe_shared_ptr<::takatori::statement::statement> const& executable_statement::statement() const noexcept {
    return statement_;
}

yugawara::compiled_info const& executable_statement::compiled_info() const noexcept {
    return compiled_info_;
}

std::shared_ptr<variable_table> const& executable_statement::host_variables() const noexcept {
    return host_variables_;
}

std::shared_ptr<variable_table_info> const& executable_statement::host_variable_info() const noexcept {
    return host_variable_info_;
}

std::shared_ptr<mirror_container> const& executable_statement::mirrors() const noexcept {
    return mirrors_;
}

bool executable_statement::is_ddl() const noexcept {
    if (! statement_) return false;
    auto k = statement_->kind();
    using takatori::statement::statement_kind;
    return
        k == statement_kind::create_table ||
        k == statement_kind::drop_table ||
        k == statement_kind::create_index ||
        k == statement_kind::drop_index;
}

std::string_view executable_statement::sql_text() const noexcept {
    if(! sql_text_) return {};
    return *sql_text_;
}

std::shared_ptr<std::string> const &executable_statement::sql_text_shared() const noexcept {
    return sql_text_;
}


} // namespace jogasaki::plan
