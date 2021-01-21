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

#include <cstddef>
#include <functional>

#include <takatori/util/object_creator.h>
#include <yugawara/compiler_result.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::plan {


executable_statement::executable_statement(
    takatori::util::unique_object_ptr<::takatori::statement::statement> statement,
    yugawara::compiled_info compiled_info, std::shared_ptr<model::statement> operators) noexcept:
    statement_(std::move(statement)),
    compiled_info_(std::move(compiled_info)),
    operators_(std::move(operators))
{}

void executable_statement::compiler_result(yugawara::compiler_result compiler_result) noexcept {
    statement_ = compiler_result.release_statement();
    compiled_info_ = std::move(compiler_result.info());
}

void executable_statement::statement(std::unique_ptr<::takatori::statement::statement> statement) noexcept {
    takatori::util::object_creator creator{};
    statement_ = creator.wrap_unique(statement.release());
}

::takatori::statement::statement const& executable_statement::statement() const noexcept {
    return *statement_;
}

void executable_statement::compiled_info(yugawara::compiled_info compiled_info) noexcept {
    compiled_info_ = std::move(compiled_info);
}

yugawara::compiled_info const& executable_statement::compiled_info() const noexcept {
    return compiled_info_;
}

void executable_statement::operators(std::shared_ptr<model::statement> operators) noexcept {
    operators_ = std::move(operators);
}

model::statement const* executable_statement::operators() const noexcept {
    return operators_.get();
}

bool executable_statement::is_execute() const noexcept {
    return statement_ && statement_->kind() == takatori::statement::statement_kind::execute;
}
}
