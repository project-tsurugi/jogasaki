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
#include "write_statement.h"

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <boost/assert.hpp>

#include <takatori/descriptor/element.h>
#include <takatori/descriptor/reference.h>
#include <takatori/descriptor/variable.h>
#include <takatori/relation/sort_direction.h>
#include <takatori/tree/tree_element_vector.h>
#include <takatori/tree/tree_fragment_vector.h>
#include <takatori/type/data.h>
#include <takatori/util/optional_ptr.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/column_value.h>
#include <yugawara/storage/column_value_kind.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/configuration.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/any.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/conv/assignment.h>
#include <jogasaki/executor/conv/create_default_value.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/default_value_kind.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/sequence/exception.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/executor/wrt/fill_record_fields.h>
#include <jogasaki/executor/wrt/insert_new_record.h>
#include <jogasaki/executor/wrt/transfer_locator.h>
#include <jogasaki/executor/wrt/write_field.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/index/secondary_target.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/abort_transaction.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/handle_encode_errors.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/make_function_context.h>

namespace jogasaki::executor::common {

using jogasaki::executor::process::impl::ops::write_kind;
using jogasaki::executor::expr::evaluator;

using takatori::util::string_builder;

constexpr static std::size_t npos = static_cast<std::size_t>(-1);

status fill_evaluated_value(
    wrt::write_field const& f,
    request_context& ctx,
    write_statement::tuple const& t,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    executor::process::impl::variable_table const* host_variables,
    data::small_record_store& out
) {
    auto& source_type = info.type_of(t.elements()[f.index_]);
    evaluator eval{t.elements()[f.index_], info, host_variables};
    process::impl::variable_table empty{};
    expr::evaluator_context c{
        std::addressof(resource),
        ctx.transaction(),
        utils::make_function_context(*ctx.transaction())
    };
    auto res = eval(c, empty, std::addressof(resource));
    if (res.error()) {
        auto err = res.to<expr::error>();
        if(err.kind() == expr::error_kind::lost_precision_value_too_long) {
            auto rc = status::err_expression_evaluation_failure;
            set_error(
                ctx,
                error_code::value_too_long_exception,
                "evaluated value was too long to write",
                rc
            );
            return rc;
        }
        if(err.kind() == expr::error_kind::unsupported) {
            auto rc = status::err_unsupported;
            set_error(
                ctx,
                error_code::unsupported_runtime_feature_exception,
                "unsupported expression",
                rc
            );
            return rc;
        }
        if(err.kind() == expr::error_kind::error_info_provided) {
            set_error_info(ctx, c.get_error_info());
            return c.get_error_info()->status();
        }
        auto rc = status::err_expression_evaluation_failure;
        set_error(
            ctx,
            error_code::value_evaluation_exception,
            string_builder{} << "An error occurred in evaluating values. error:"
                             << res.to<expr::error>() << string_builder::to_string,
            rc
        );
        return rc;
    }

    // To clean up varlen data resource in data::any, we rely on upper layer that does clean up
    // on evey process invocation. Otherwise, we have to copy the result of conversion and
    // lifo resource is not convenient to copy the result when caller and callee use the same resource.
    data::any converted{res};
    if(conv::to_require_conversion(source_type, *f.target_type_)) {
        if(auto st = conv::conduct_assignment_conversion(
            source_type,
            *f.target_type_,
            res,
            converted,
            ctx,
            std::addressof(resource)
        );
        st != status::ok) {
            return st;
        }
    }
    // varlen fields data is already on `resource`, so no need to copy
    auto nocopy = nullptr;
    if (f.nullable_) {
        utils::copy_nullable_field(f.type_, out.ref(), f.offset_, f.nullity_offset_, converted, nocopy);
    } else {
        if (!converted) {
            auto rc = status::err_integrity_constraint_violation;
            set_error(
                ctx,
                error_code::not_null_constraint_violation_exception,
                string_builder{} << "Null assigned for non-nullable field." << string_builder::to_string,
                rc);
            return rc;
        }
        utils::copy_field(f.type_, out.ref(), f.offset_, converted, nocopy);
    }
    wrt::transfer_blob_locators(ctx, c);
    return status::ok;
}

status create_record_from_tuple(  //NOLINT(readability-function-cognitive-complexity)
    request_context& ctx,
    write_statement::tuple const& t,
    std::vector<wrt::write_field> const& fields,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    executor::process::impl::variable_table const* host_variables,
    data::small_record_store& out
) {
    for (auto&& f: fields) {
        if (f.index_ == npos) {
            // value not specified for the field use default value or null
            if(auto res = fill_default_value(f, ctx, resource, out); res != status::ok) {
                return res;
            }
            continue;
        }
        if(auto res = fill_evaluated_value(f, ctx, t, info, resource, host_variables, out); res != status::ok) {
            return res;
        }
    }
    return status::ok;
}

write_statement::write_statement(
    write_kind kind,
    yugawara::storage::index const& idx,
    takatori::statement::write const& wrt,
    memory::lifo_paged_memory_resource& resource,
    compiled_info info,
    executor::process::impl::variable_table const* host_variables
) :
    kind_(kind),
    idx_(std::addressof(idx)),
    wrt_(std::addressof(wrt)),
    resource_(std::addressof(resource)),
    info_(std::move(info)),
    host_variables_(host_variables),
    key_meta_(index::create_meta(*idx_, true)),
    value_meta_(index::create_meta(*idx_, false)),
    key_fields_(wrt::create_fields(*idx_, wrt_->columns(), key_meta_, value_meta_, true, resource_)),
    value_fields_(wrt::create_fields(*idx_, wrt_->columns(), key_meta_, value_meta_, false, resource_)),
    entity_(std::make_shared<wrt::insert_new_record>(
        kind_,
        wrt::create_primary_target(idx_->simple_name(), key_meta_, value_meta_, key_fields_, value_fields_),
        wrt::create_secondary_targets(*idx_, key_meta_, value_meta_)
    ))
{}

model::statement_kind write_statement::kind() const noexcept {
    return model::statement_kind::write;
}

bool write_statement::operator()(request_context& context) {
    auto res = process(context);
    if(! res) {
        // Ensure tx aborts on any error. Though tx might be already aborted due to kvs errors,
        // aborting again will do no harm since sharksfin tx manages is_active flag and omits aborting again.
        auto& tx = context.transaction();
        utils::abort_transaction(*tx);
    }
    return res;
}
bool write_statement::process(request_context& context) {
    auto& tx = context.transaction();
    BOOST_ASSERT(tx);  //NOLINT
    auto* db = tx->database();

    wrt::write_context wctx(context,
        idx_->simple_name(),
        key_meta_,
        value_meta_,
        entity_->secondaries(),
        *db,
        resource_); // currently common::write uses the same resource for building mirror and executing runtime

    for(auto&& tuple: wrt_->tuples()) {
        utils::checkpoint_holder cph(resource_);
        if(auto res = create_record_from_tuple(
            context,
            tuple,
            key_fields_,
            info_,
            *resource_,
            host_variables_,
            wctx.key_store_
        ); res != status::ok) {
            return false;
        }
        if(auto res = create_record_from_tuple(
            context,
            tuple,
            value_fields_,
            info_,
            *resource_,
            host_variables_,
            wctx.value_store_
        ); res != status::ok) {
            return false;
        }
        if(! entity_->process_record(context, wctx)) {
            return false;
        }
    }
    return true;
}
}  // namespace jogasaki::executor::common
