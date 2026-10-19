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
#include "record_assignment.h"

#include <utility>

#include <jogasaki/data/any.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/conv/assignment.h>
#include <jogasaki/executor/conv/require_conversion.h>
#include <jogasaki/executor/conv/unify.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::value_operations {

record_destination record_destination::from_record(meta::record_meta const& schema, std::size_t index) {
    return {schema.at(index), {schema.value_offset(index), schema.nullity_offset(index)}};
}

namespace details {
record_transfer::record_transfer(
    takatori::type::data const& source_type, takatori::type::data const& target_type,
    meta::field_type source_storage, record_position source, record_destination target)
    : source_type_(&source_type), target_type_(&target_type), source_storage_(std::move(source_storage)),
      source_(source), target_(std::move(target)),
      requires_conversion_(executor::conv::to_require_conversion(source_type, target_type)) {}

template<class Convert>
status record_transfer::apply_borrowed(
    accessor::const_record_ref source, accessor::record_ref target, Convert const& convert) const {
    if (!requires_conversion_) {
        utils::copy_nullable_field(target_.storage_type, target, target_.position.value_offset,
            target_.position.nullity_offset, source, source_.value_offset, source_.nullity_offset);
        return status::ok;
    }
    data::any value{};
    utils::copy_nullable_field_as_any(source_storage_, source, source_.value_offset,
        source_.nullity_offset, value, nullptr);
    data::any converted{};
    if (auto st = convert(*source_type_, *target_type_, value, converted); st != status::ok) {
        return st;
    }
    utils::copy_nullable_field(target_.storage_type, target, target_.position.value_offset,
        target_.position.nullity_offset, converted);
    return status::ok;
}
} // namespace details

prepared_record_assignment::prepared_record_assignment(details::record_transfer transfer, bool target_nullable)
    : transfer_(std::move(transfer)), target_nullable_(target_nullable) {}

prepared_record_assignment prepared_record_assignment::prepare(
    takatori::type::data const& source_type, takatori::type::data const& target_type,
    record_position source, record_position target, bool target_nullable) {
    auto source_storage = utils::type_for(source_type);
    auto target_storage = utils::type_for(target_type);
    return prepared_record_assignment{details::record_transfer{source_type, target_type,
        std::move(source_storage), source, record_destination{std::move(target_storage), target}}, target_nullable};
}

status prepared_record_assignment::apply_borrowed(
    accessor::const_record_ref source, accessor::record_ref target, request_context& context,
    memory::lifo_paged_memory_resource* storage) const {
    if (source.is_null(transfer_.source_.nullity_offset) && !target_nullable_) {
        set_error_context(context, error_code::not_null_constraint_violation_exception,
            "Null assigned for non-nullable field.", status::err_integrity_constraint_violation);
        return status::err_integrity_constraint_violation;
    }
    return transfer_.apply_borrowed(source, target,
        [&](takatori::type::data const& from, takatori::type::data const& to,
            data::any const& value, data::any& result) {
            return executor::conv::conduct_assignment_conversion(from, to, value, result, context, storage);
        });
}

prepared_record_unification::prepared_record_unification(details::record_transfer transfer)
    : transfer_(std::move(transfer)) {}

prepared_record_unification prepared_record_unification::prepare(
    takatori::type::data const& source_type, takatori::type::data const& target_type,
    record_position source, record_destination target) {
    auto source_storage = utils::type_for(source_type);
    return prepared_record_unification{details::record_transfer{
        source_type, target_type, std::move(source_storage), source, std::move(target)}};
}

status prepared_record_unification::apply_borrowed(
    accessor::const_record_ref source, accessor::record_ref target, memory::lifo_paged_memory_resource* storage) const {
    return transfer_.apply_borrowed(source, target,
        [&](takatori::type::data const& from, takatori::type::data const& to,
            data::any const& value, data::any& result) {
            return executor::conv::conduct_unifying_conversion(from, to, value, result, storage);
        });
}

} // namespace jogasaki::value_operations
