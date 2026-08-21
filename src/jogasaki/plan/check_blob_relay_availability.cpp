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
#include "check_blob_relay_availability.h"

#include <string>
#include <string_view>

#include <takatori/plan/graph.h>
#include <takatori/plan/process.h>
#include <takatori/plan/step.h>
#include <takatori/plan/step_kind.h>
#include <takatori/relation/apply.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/graph.h>
#include <takatori/scalar/expression.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/function_call.h>
#include <takatori/statement/execute.h>
#include <takatori/statement/statement_kind.h>
#include <takatori/type/data.h>
#include <takatori/type/table.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <takatori/util/exception.h>
#include <takatori/util/string_builder.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/binding/extract.h>
#include <yugawara/function/declaration.h>

#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/function/scalar_function_info.h>
#include <jogasaki/executor/function/scalar_function_kind.h>
#include <jogasaki/executor/function/scalar_function_repository.h>
#include <jogasaki/executor/function/table_valued_function_info.h>
#include <jogasaki/executor/function/table_valued_function_kind.h>
#include <jogasaki/executor/function/table_valued_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/plan/plan_exception.h>
#include <jogasaki/status.h>

namespace jogasaki::plan {

using takatori::util::string_builder;
using takatori::util::throw_exception;
using takatori::util::unsafe_downcast;

namespace {

bool is_lob(::takatori::type::data const& type) noexcept {
    return type.kind() == ::takatori::type::type_kind::blob ||
           type.kind() == ::takatori::type::type_kind::clob;
}

bool is_user_defined_scalar_function(::yugawara::function::declaration::definition_id_type id) {
    auto const* info = global::scalar_function_repository().find(id);
    return info != nullptr &&
           info->kind() == executor::function::scalar_function_kind::user_defined;
}

bool is_user_defined_table_valued_function(::yugawara::function::declaration::definition_id_type id) {
    auto const* info = global::table_valued_function_repository().find(id);
    return info != nullptr &&
           info->kind() == executor::function::table_valued_function_kind::user_defined;
}

[[noreturn]] void raise_unavailable(std::string_view function_name) {
    throw_exception(plan_exception{create_error_info(
        error_code::service_unavailable,
        string_builder{} << "BLOB relay service is unavailable, so the function \"" << function_name
                         << "\" handling BLOB/CLOB cannot be used" << string_builder::to_string,
        status::err_unsupported
    )});
}

/**
 * @brief check whether the function signature handles LOB
 * @details LOB appearing in the parameters or in the return value (including the columns of the
 * table returned by a table valued function) is transferred via the BLOB relay service.
 */
bool signature_contains_lob(::yugawara::function::declaration const& decl) {
    for (auto&& p : decl.parameter_types()) {
        if (is_lob(p)) {
            return true;
        }
    }
    auto&& ret = decl.return_type();
    if (is_lob(ret)) {
        return true;
    }
    if (ret.kind() == ::takatori::type::type_kind::table) {
        for (auto&& c : unsafe_downcast<::takatori::type::table>(ret).columns()) {
            if (is_lob(c.type())) {
                return true;
            }
        }
    }
    return false;
}

}  // namespace

void check_blob_relay_availability(
    ::takatori::statement::statement const& statement,
    ::yugawara::compiled_info const& info
) {
    if (global::relay_service() != nullptr) {
        // the BLOB relay service is available, so nothing to check
        return;
    }

    // scalar function calls appearing anywhere in the statement
    info.expressions().each([](::takatori::scalar::expression const& e,
                                ::yugawara::analyzer::expression_resolution const&) {
        if (e.kind() != ::takatori::scalar::expression_kind::function_call) {
            return;
        }
        auto&& fc = unsafe_downcast<::takatori::scalar::function_call>(e);
        auto decl = ::yugawara::binding::extract_if<::yugawara::function::declaration>(fc.function());
        if (! decl.has_value()) {
            return;
        }
        if (! is_user_defined_scalar_function(decl->definition_id())) {
            return;
        }
        if (signature_contains_lob(*decl)) {
            raise_unavailable(decl->name());
        }
    });

    // table valued function invocations (i.e. APPLY clause) are relation operators
    if (statement.kind() != ::takatori::statement::statement_kind::execute) {
        return;
    }
    auto&& execute = unsafe_downcast<::takatori::statement::execute>(statement);
    ::takatori::plan::sort_from_upstream(
        execute.execution_plan(),
        [](::takatori::plan::step const& s) {
            if (s.kind() != ::takatori::plan::step_kind::process) {
                return;
            }
            auto&& process = unsafe_downcast<::takatori::plan::process const>(s);
            ::takatori::relation::sort_from_upstream(
                process.operators(),
                [](::takatori::relation::expression const& op) {
                    if (op.kind() != ::takatori::relation::expression_kind::apply) {
                        return;
                    }
                    auto&& a = unsafe_downcast<::takatori::relation::apply>(op);
                    auto decl = ::yugawara::binding::extract_if<::yugawara::function::declaration>(a.function());
                    if (! decl.has_value()) {
                        return;
                    }
                    if (! is_user_defined_table_valued_function(decl->definition_id())) {
                        return;
                    }
                    if (signature_contains_lob(*decl)) {
                        raise_unavailable(decl->name());
                    }
                }
            );
        }
    );
}

}  // namespace jogasaki::plan
