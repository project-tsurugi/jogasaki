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

#include <cstdlib>
#include <ostream>
#include <string_view>

#include <takatori/type/data.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/conv/assignment.h>
#include <jogasaki/executor/conv/require_conversion.h>
#include <jogasaki/executor/conv/unify.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/relay/blob_session_container.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/copy_field_data.h>

namespace jogasaki::executor::wrt {

/**
 * @brief specifies which type conversion to apply after expression evaluation in fill_evaluated_value.
 */
enum class value_input_conversion_kind {
    /// @brief no conversion; an error is raised if the source and target types differ.
    none,
    /// @brief apply assignment conversion (used when writing to table columns).
    assignment,
    /// @brief apply unifying conversion (used for VALUES clause rows).
    unify,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(value_input_conversion_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = value_input_conversion_kind;
    switch (value) {
        case kind::none: return "none"sv;
        case kind::assignment: return "assignment"sv;
        case kind::unify: return "unify"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, value_input_conversion_kind value) {
    return out << to_string_view(value);
}

/**
 * @brief evaluates a scalar expression and writes the result into a record field.
 * @details Evaluates `expr` using `info`, applies assignment conversion from the
 *          expression's source type to `target_type` when necessary, and then writes
 *          the result into `out` at the offsets described by `field`.
 *
 *          The `Field` template parameter must provide the following members:
 *          - `meta::field_type type_`                    : field type for copy helpers
 *          - `bool nullable_`                             : whether the field accepts NULL
 *          - `std::size_t offset_`                        : byte offset of the value in `out`
 *          - `std::size_t nullity_offset_`                : bit offset of the nullity flag in `out`
 *          - `takatori::type::data const* target_type_`   : target type for assignment conversion
 *
 * @tparam Field field descriptor type providing the members listed above.
 * @param eval pre-constructed evaluator for the expression.
 * @param source_type the type of the expression as determined by the SQL compiler.
 * @param conversion specifies which type of conversion to apply after evaluation.
 * @param field field descriptor that identifies where and how to write the result.
 * @param ctx request context used for error reporting and transaction access.
 * @param blob_session blob session container used by the evaluator for BLOB handling.
 * @param variables the working variable table for resolving variable references in the expression.
 * @param resource memory resource for expression evaluation and varlen data allocation.
 * @param out target record reference to write the evaluated value into.
 * @return status::ok on success, or an error status on failure.
 */
template <typename Field>
status fill_evaluated_value(
    expr::evaluator const& eval,
    takatori::type::data const& source_type,
    value_input_conversion_kind conversion,
    Field const& field,
    request_context& ctx,
    relay::blob_session_container& blob_session,
    executor::process::impl::variable_table& variables,
    memory::lifo_paged_memory_resource& resource,
    accessor::record_ref out
) {
    using takatori::util::string_builder;

    expr::evaluator_context c{
        std::addressof(resource),
        ctx.transaction().get()
    };
    c.blob_session(std::addressof(blob_session));
    auto res = eval(c, variables, std::addressof(resource));
    if (res.error()) {
        auto err = res.to<expr::error>();
        if (err.kind() == expr::error_kind::lost_precision_value_too_long) {
            auto rc = status::err_expression_evaluation_failure;
            set_error_context(
                ctx,
                error_code::value_too_long_exception,
                "evaluated value was too long to write",
                rc
            );
            return rc;
        }
        if (err.kind() == expr::error_kind::unsupported) {
            auto rc = status::err_unsupported;
            set_error_context(
                ctx,
                error_code::unsupported_runtime_feature_exception,
                "unsupported expression",
                rc
            );
            return rc;
        }
        if (err.kind() == expr::error_kind::error_info_provided) {
            set_error_info(ctx, c.get_error_info());
            return c.get_error_info()->status();
        }
        auto rc = status::err_expression_evaluation_failure;
        set_error_context(
            ctx,
            error_code::value_evaluation_exception,
            string_builder{} << "An error occurred in evaluating values. error:"
                             << res.to<expr::error>() << string_builder::to_string,
            rc
        );
        return rc;
    }

    // To clean up varlen data resource in data::any, we rely on upper layer that does clean up
    // on every process invocation. Otherwise, we have to copy the result of conversion and
    // lifo resource is not convenient to copy the result when caller and callee use the same resource.
    data::any converted{res};
    if (conv::to_require_conversion(source_type, *field.target_type_)) {
        if (conversion == value_input_conversion_kind::none) {
            auto rc = status::err_expression_evaluation_failure;
            set_error_context(
                ctx,
                error_code::value_evaluation_exception,
                string_builder{} << "type conversion not expected but source and target types differ"
                                 << string_builder::to_string,
                rc
            );
            return rc;
        }
        if (conversion == value_input_conversion_kind::assignment) {
            if (auto st = conv::conduct_assignment_conversion(
                    source_type,
                    *field.target_type_,
                    res,
                    converted,
                    ctx,
                    std::addressof(resource)
                );
                st != status::ok) {
                return st;
            }
        } else {
            if (auto st = conv::conduct_unifying_conversion(
                    source_type,
                    *field.target_type_,
                    res,
                    converted,
                    std::addressof(resource)
                );
                st != status::ok) {
                return st;
            }
        }
    }
    // varlen fields data is already on `resource`, so no need to copy
    auto nocopy = nullptr;
    if (field.nullable_) {
        utils::copy_nullable_field(field.type_, out, field.offset_, field.nullity_offset_, converted, nocopy);
    } else {
        if (! converted) {
            auto rc = status::err_integrity_constraint_violation;
            set_error_context(
                ctx,
                error_code::not_null_constraint_violation_exception,
                string_builder{} << "Null assigned for non-nullable field." << string_builder::to_string,
                rc
            );
            return rc;
        }
        utils::copy_field(field.type_, out, field.offset_, converted, nocopy);
    }
    return status::ok;
}

}  // namespace jogasaki::executor::wrt
