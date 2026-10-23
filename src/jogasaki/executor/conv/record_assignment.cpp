/*
 * Copyright 2026 Project Tsurugi.
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
#include "record_assignment.h"
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/conv/assignment.h>
#include <jogasaki/executor/conv/require_conversion.h>
#include <jogasaki/executor/conv/unify.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::conv {
namespace {
status reject_null(request_context* context) {
    if (context) set_error_context(*context, error_code::not_null_constraint_violation_exception,
        "Null assigned for non-nullable field.", status::err_integrity_constraint_violation);
    return status::err_integrity_constraint_violation;
}
}
record_assignment::record_assignment(takatori::type::data const& source, takatori::type::data const& target,
    data::field_operations destination, field_position position, bool nullable, conversion_kind conversion)
    : source_type_(&source), target_type_(&target), destination_(destination), position_(position),
      nullable_(nullable), conversion_(conversion), requires_conversion_(to_require_conversion(source, target)) {}
record_assignment record_assignment::prepare_value(takatori::type::data const& source,
    takatori::type::data const& target, meta::field_type const& storage, field_position destination,
    bool nullable, conversion_kind conversion) {
    return {source, target, data::field_operations::for_type(storage), destination, nullable, conversion};
}
record_assignment record_assignment::prepare_record(takatori::type::data const& source,
    takatori::type::data const& target, meta::field_type const& source_storage, field_position origin,
    meta::field_type const& target_storage, field_position destination, bool nullable,
    conversion_kind conversion) {
    auto result = prepare_value(source, target, target_storage, destination, nullable, conversion);
    result.source_.emplace(record_source{data::field_operations::for_type(source_storage), origin});
    return result;
}
status record_assignment::apply(data::any source, accessor::record_ref target,
    request_context* context, memory::lifo_paged_memory_resource* resource) const {
    if (source.error()) {
        if (context) set_error_context(*context, error_code::value_evaluation_exception,
            "Cannot assign an evaluation error.", status::err_expression_evaluation_failure);
        return status::err_expression_evaluation_failure;
    }
    auto converted = source;
    if (requires_conversion_) {
        auto result = status::ok;
        switch (conversion_) {
            case conversion_kind::assignment:
                if (!context) return status::err_invalid_state;
                result = conduct_assignment_conversion(*source_type_, *target_type_, source, converted, *context, resource);
                break;
            case conversion_kind::unify:
                result = conduct_unifying_conversion(*source_type_, *target_type_, source, converted, resource);
                break;
            case conversion_kind::none:
                if (context) set_error_context(*context, error_code::value_evaluation_exception,
                    "type conversion not expected but source and target types differ", status::err_expression_evaluation_failure);
                return status::err_expression_evaluation_failure;
        }
        if (result != status::ok) return result;
    }
    if (converted.empty() && !nullable_) return reject_null(context);
    if (nullable_) destination_.copy_nullable(target, position_.value, position_.nullity, converted);
    else destination_.copy(target, position_.value, converted);
    return status::ok;
}
status record_assignment::apply(accessor::const_record_ref source, accessor::record_ref target,
    request_context* context, memory::lifo_paged_memory_resource* resource) const {
    if (!source_) fail_with_exception();
    auto const& origin = *source_;
    auto is_null = source.is_null(origin.position.nullity);
    if (is_null && !nullable_) return reject_null(context);
    if (!requires_conversion_) {
        if (nullable_) destination_.copy_nullable(target, position_.value, position_.nullity,
            source, origin.position.value, origin.position.nullity);
        else destination_.copy(target, position_.value, source, origin.position.value);
        return status::ok;
    }
    data::any input;
    origin.operations.read_nullable(source, origin.position.value, origin.position.nullity, input);
    return apply(input, target, context, resource);
}
}
