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
#include "find.h"

#include <cstddef>
#include <utility>
#include <vector>
#include <boost/assert.hpp>

#include <takatori/descriptor/element.h>
#include <takatori/relation/sort_direction.h>
#include <takatori/util/downcast.h>
#include <takatori/util/infect_qualifier.h>
#include <takatori/util/reference_iterator.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/column.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/details/search_key_field_info.h>
#include <jogasaki/executor/process/impl/ops/index_field_mapper.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/index/field_factory.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/request_cancel_config.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/cancel_request.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/modify_status.h>
#include <jogasaki/utils/set_cancel_status.h>

#include "context_helper.h"
#include "details/encode_key.h"
#include "details/error_abort.h"
#include "find_context.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

find::find(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::string_view storage_name,
    std::string_view secondary_storage_name,
    std::vector<details::search_key_field_info> search_key_fields,
    std::vector<index::field_info> key_fields,
    std::vector<index::field_info> value_fields,
    std::vector<details::secondary_index_field_info> secondary_key_fields,
    std::unique_ptr<operator_base> downstream,
    variable_table_info const* input_variable_info,
    variable_table_info const* output_variable_info
) :
    record_operator(index, info, block_index, input_variable_info, output_variable_info),
    use_secondary_(! secondary_storage_name.empty()),
    storage_name_(storage_name),
    secondary_storage_name_(secondary_storage_name),
    search_key_fields_(std::move(search_key_fields)),
    downstream_(std::move(downstream)),
    field_mapper_(
        use_secondary_,
        std::move(key_fields),
        std::move(value_fields),
        std::move(secondary_key_fields)
    )
{}

find::find(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    takatori::tree::tree_fragment_vector<key> const& keys,
    yugawara::storage::index const& primary_idx,
    sequence_view<column const> columns,
    yugawara::storage::index const* secondary_idx,
    std::unique_ptr<operator_base> downstream,
    variable_table_info const* input_variable_info,
    variable_table_info const* output_variable_info
) :
    find(
        index,
        info,
        block_index,
        primary_idx.simple_name(),
        secondary_idx != nullptr ? secondary_idx->simple_name() : "",
        details::create_search_key_fields((secondary_idx != nullptr ? *secondary_idx : primary_idx), keys, info),
        index::create_fields<column>(primary_idx, columns, (output_variable_info != nullptr ? *output_variable_info : info.vars_info_list()[block_index]), true, true),
        index::create_fields<column>(primary_idx, columns, (output_variable_info != nullptr ? *output_variable_info : info.vars_info_list()[block_index]), false, true),
        create_secondary_key_fields(secondary_idx),
        std::move(downstream),
        input_variable_info,
        output_variable_info
    )
{}

operation_status find::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<class find_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<class find_context>(index(),
            ctx.variable_table(block_index()),
            ctx.variable_table(block_index()),
            ctx.database()->get_storage(storage_name()),
            use_secondary_ ? ctx.database()->get_storage(secondary_storage_name()) : nullptr,
            ctx.transaction(),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status find::call_downstream(
    class find_context& ctx,
    std::string_view k,
    std::string_view v,
    accessor::record_ref target,
    context_base::memory_resource* resource,
    abstract::task_context* context
) {
    if (auto res = field_mapper_(k, v, target, *ctx.stg_, *ctx.tx_, resource); res != status::ok) {
        return error_abort(ctx, res);
    }
    if (downstream_) {
        if(auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); !st) {
            ctx.abort();
            return {operation_status_kind::aborted};
        }
    }
    return {};
}

operation_status find::operator()(class find_context& ctx, abstract::task_context* context) {  //NOLINT(readability-function-cognitive-complexity)
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    if(utils::request_cancel_enabled(request_cancel_kind::find) && ctx.req_context()) {
        auto res_src = ctx.req_context()->req_info().response_source();
        if(res_src && res_src->check_cancel()) {
            set_cancel_status(*ctx.req_context());
            ctx.abort();
            finish(context);
            return {operation_status_kind::aborted};
        }
    }
    auto target = ctx.output_variables().store().ref();
    auto resource = ctx.varlen_resource();
    std::string_view v{};
    executor::process::impl::variable_table vars{};
    std::size_t len{};
    std::string msg{};
    if(auto res = details::encode_key(*ctx.req_context(), search_key_fields_, vars, *resource, ctx.key_, len, msg);
        res != status::ok) {
        if (res == status::err_type_mismatch) {
            // unsupported type/value mapping detected during expression evaluation
            ctx.abort();
            set_error(
                *ctx.req_context(),
                error_code::unsupported_runtime_feature_exception,
                msg,
                res
            );
            return {operation_status_kind::aborted};
        }
        if (res == status::err_integrity_constraint_violation) {
            // null is assigned for find condition. Nothing should be found.
            finish(context);
            return {};
        }
        return error_abort(ctx, res);
    }
    std::string_view k{static_cast<char*>(ctx.key_.data()), len};
    if (! use_secondary_) {
        auto& stg = *ctx.stg_;
        if(auto res = stg.content_get(*ctx.tx_, k, v); res != status::ok) {
            finish(context);
            utils::modify_concurrent_operation_status(*ctx.tx_, res, false);
            if (res == status::not_found) {
                return {};
            }
            handle_kvs_errors(*ctx.req_context(), res);
            return error_abort(ctx, res);
        }
        auto ret = call_downstream(ctx, k, v, target, resource, context);
        finish(context);
        return ret;
    }
    auto& stg = *ctx.secondary_stg_;
    std::unique_ptr<kvs::iterator> it{};
    if(auto res = stg.content_scan(*ctx.tx_,
            k, kvs::end_point_kind::prefixed_inclusive,
            k, kvs::end_point_kind::prefixed_inclusive,
            it
        ); res != status::ok) {
        finish(context);
        handle_kvs_errors(*ctx.req_context(), res);
        return error_abort(ctx, res);
    }
    while(true) {
        if(auto res = it->next(); res != status::ok) {
            finish(context);
            if (res == status::not_found) {
                return {};
            }
            handle_kvs_errors(*ctx.req_context(), res);
            return error_abort(ctx, res);
        }
        if(auto res = it->read_key(k); res != status::ok) {
            utils::modify_concurrent_operation_status(*ctx.tx_, res, true);
            // shirakami returns error here even if next() above returns ok
            // (e.g. not_found for concurrently deleted entry or concurrent_operation for concurrently inserted)
            // skip the record and continue to next
            if (res == status::not_found) {
                continue;
            }
            finish(context);
            handle_kvs_errors(*ctx.req_context(), res);
            return error_abort(ctx, res);
        }
        if(auto ret = call_downstream(ctx, k, v, target, resource, context); ! ret) {
            finish(context);
            return ret;
        }
    }
    finish(context);
    return {};
}

operator_kind find::kind() const noexcept {
    return operator_kind::find;
}

std::string_view find::storage_name() const noexcept {
    return storage_name_;
}

std::string_view find::secondary_storage_name() const noexcept {
    return secondary_storage_name_;
}

void find::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if(auto* p = find_context<class find_context>(index(), ctx.contexts())) {
        p->release();
    }
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

std::vector<details::secondary_index_field_info> find::create_secondary_key_fields(
    yugawara::storage::index const* idx
) {
    if (idx == nullptr) {
        return {};
    }
    std::vector<details::secondary_index_field_info> ret{};
    ret.reserve(idx->keys().size());
    yugawara::binding::factory bindings{};
    for(auto&& k : idx->keys()) {
        auto kc = bindings(k.column());
        auto t = utils::type_for(k.column().type());
        auto spec = k.direction() == relation::sort_direction::ascendant ?
            kvs::spec_key_ascending : kvs::spec_key_descending; // no storage spec with fields for read
        ret.emplace_back(
            t,
            k.column().criteria().nullity().nullable(),
            spec
        );
    }
    return ret;
}

}  // namespace jogasaki::executor::process::impl::ops
