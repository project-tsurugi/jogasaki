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
#include "write_partial.h"

#include <vector>

#include <takatori/relation/write.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/logging.h>
#include <jogasaki/error.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/utils/field_types.h>
#include "operator_base.h"
#include "context_helper.h"
#include "details/error_abort.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using variable = takatori::descriptor::variable;

void write_partial::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if(auto* p = find_context<write_partial_context>(index(), ctx.contexts())) {
        p->release();
    }
}

std::string_view write_partial::storage_name() const noexcept {
    return primary_->storage_name();
}

operator_kind write_partial::kind() const noexcept {
    return operator_kind::write_partial;
}

operation_status write_partial::operator()(write_partial_context& ctx) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    auto& context = *ctx.primary_context_;
    // find update target and fill ctx.key_store_ and ctx.value_store_
    if(auto res = primary_->find_record_and_extract(
            context,
            *ctx.transaction(),
            ctx.input_variables().store().ref(),
            ctx.varlen_resource()
        ); res != status::ok) {
        return details::error_abort(ctx, res);
    }

    // update fields in key_store_/value_store_ with values from variable table
    primary_->update_record(
        context,
        ctx.input_variables().store().ref(),
        host_variables() ? host_variables()->store().ref() : accessor::record_ref{}
    );

    // encode values from key_store_/value_store_ and send to kvs
    if(auto res = primary_->encode_and_put(context, *ctx.transaction()); res != status::ok) {
        return details::error_abort(ctx, res);
    }
    return {};
}

operation_status write_partial::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<write_partial_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<write_partial_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.database()->get_storage(storage_name()),
            ctx.transaction(),
            primary_->key_meta(),
            primary_->value_meta(),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p);
}

write_partial::write_partial(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    write_kind kind,
    std::string_view storage_name,
    yugawara::storage::index const& idx,
    sequence_view<key const> keys,
    sequence_view<column const> columns,
    variable_table_info const* input_variable_info
) :
    write_partial(
        index,
        info,
        block_index,
        kind,
        std::make_unique<details::primary_target>(
            storage_name,
            idx,
            keys,
            columns,
            input_variable_info ? input_variable_info : &info.vars_info_list()[block_index],
            info.host_variables() ? std::addressof(info.host_variables()->info()) : nullptr
        ),
        input_variable_info
    )
{}

write_partial::write_partial(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    write_kind kind,
    std::unique_ptr<details::primary_target> primary,
    variable_table_info const* input_variable_info
) :
    record_operator(index, info, block_index, input_variable_info),
    kind_(kind),
    primary_(std::move(primary))
{}

}
