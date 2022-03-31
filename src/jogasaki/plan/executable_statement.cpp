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
#include "executable_statement.h"

#include <yugawara/compiler_result.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/model/statement.h>

namespace jogasaki::plan {

bool executable_statement::is_execute() const noexcept {
    return statement_ && statement_->kind() == takatori::statement::statement_kind::execute;
}

executable_statement::executable_statement(
    maybe_shared_ptr<::takatori::statement::statement> statement,
    yugawara::compiled_info compiled_info,
    maybe_shared_ptr<model::statement> operators,
    std::shared_ptr<variable_table_info> host_variable_info,
    std::shared_ptr<variable_table> host_variables,
    std::shared_ptr<mirror_container> mirrors
) noexcept:
    statement_(std::move(statement)),
    compiled_info_(std::move(compiled_info)),
    operators_(std::move(operators)),
    host_variable_info_(std::move(host_variable_info)),
    host_variables_(std::move(host_variables)),
    mirrors_(std::move(mirrors))
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

}