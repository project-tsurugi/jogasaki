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
#include <takatori/util/exception.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/logging.h>
#include <jogasaki/error.h>
#include <jogasaki/request_context.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/utils/field_types.h>
#include "operator_base.h"
#include "context_helper.h"
#include "details/error_abort.h"
#include "jogasaki/logging_helper.h"

namespace jogasaki::executor::process::impl::ops {

using variable = takatori::descriptor::variable;
using takatori::util::throw_exception;

void write_partial::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if(auto* p = find_context<write_partial_context>(index(), ctx.contexts())) {
        p->release();
    }
}

std::string_view write_partial::storage_name() const noexcept {
    return primary_.storage_name();
}

operator_kind write_partial::kind() const noexcept {
    return operator_kind::write_partial;
}

operation_status write_partial::operator()(write_partial_context& ctx) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    switch(kind_) {
        case write_kind::update:
            return do_update(ctx);
        case write_kind::delete_:
            return do_delete(ctx);
        default:
            fail();
    }
}

void abort_transaction(transaction_context& tx) {
    if (auto res = tx.abort(); res != status::ok) {
        throw_exception(std::logic_error{"abort failed unexpectedly"});
    }
}

operation_status write_partial::do_update(write_partial_context& ctx) {
    auto& context = ctx.primary_context();
    // find update target and fill ctx.key_store_ and ctx.value_store_
    if(auto res = primary_.find_record_and_remove(
            context,
            *ctx.transaction(),
            ctx.input_variables().store().ref(),
            ctx.varlen_resource()
        ); res != status::ok) {
        abort_transaction(*ctx.transaction());
        return details::error_abort(ctx, res);
    }

    for(std::size_t i=0, n=secondaries_.size(); i<n; ++i) {
        if(auto res = secondaries_[i].encode_and_remove(
            ctx.secondary_contexts_[i],
            *ctx.transaction(),
            context.extracted_key(),
            context.extracted_value(),
            context.encoded_key()
        ); res != status::ok) {
            abort_transaction(*ctx.transaction());
            return details::error_abort(ctx, res);
        }
    }

    // update fields in key_store_/value_store_ with values from variable table
    primary_.update_record(
        context,
        ctx.input_variables().store().ref(),
        host_variables() ? host_variables()->store().ref() : accessor::record_ref{}
    );

    // encode values from key_store_/value_store_ and send to kvs
    if(auto res = primary_.encode_and_put(context, *ctx.transaction()); res != status::ok) {
        abort_transaction(*ctx.transaction());
        if(res == status::already_exists) {
            res = status::err_unique_constraint_violation;
        }
        return details::error_abort(ctx, res);
    }

    for(std::size_t i=0, n=secondaries_.size(); i<n; ++i) {
        if(auto res = secondaries_[i].encode_and_put(
                ctx.secondary_contexts_[i],
                *ctx.transaction(),
                context.extracted_key(),
                context.extracted_value(),
                context.encoded_key()
            ); res != status::ok) {
            abort_transaction(*ctx.transaction());
            return details::error_abort(ctx, res);
        }
    }
    return {};
}

operation_status write_partial::do_delete(write_partial_context& ctx) {
    auto& context = ctx.primary_context();
    if(secondaries_.empty()) {
        if(auto res = primary_.remove_record(
                context,
                *ctx.transaction(),
                ctx.input_variables().store().ref()
            ); res != status::ok) {
            return details::error_abort(ctx, res);
        }
        return {};
    }

    // find update target and fill ctx.key_store_ and ctx.value_store_ to delete from secondaries
    if(auto res = primary_.find_record_and_remove(
            context,
            *ctx.transaction(),
            ctx.input_variables().store().ref(),
            ctx.varlen_resource()
        ); res != status::ok) {
        return details::error_abort(ctx, res);
    }

    for(std::size_t i=0, n=secondaries_.size(); i<n; ++i) {
        if(auto res = secondaries_[i].encode_and_remove(
                ctx.secondary_contexts_[i],
                *ctx.transaction(),
                context.extracted_key(),
                context.extracted_value(),
                context.encoded_key()
            ); res != status::ok) {
            return details::error_abort(ctx, res);
        }
    }
    return {};
}
operation_status write_partial::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<write_partial_context>(index(), ctx.contexts());
    if (! p) {
        std::vector<details::write_secondary_context> contexts{};
        contexts.reserve(secondaries_.size());
        for(auto&& s : secondaries_) {
            contexts.emplace_back(ctx.database()->get_or_create_storage(s.storage_name()));
        }
        p = ctx.make_context<write_partial_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.database()->get_storage(storage_name()),
            ctx.transaction(),
            primary_.key_meta(),
            primary_.value_meta(),
            ctx.resource(),
            ctx.varlen_resource(),
            std::move(contexts)
        );
    }
    return (*this)(*p);
}

write_partial::write_partial(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    write_kind kind,
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
        details::write_primary_target{
            idx,
            keys,
            columns,
            input_variable_info ? *input_variable_info : info.vars_info_list()[block_index],
            info.host_variables() ? std::addressof(info.host_variables()->info()) : nullptr
        },
        create_secondary_targets(idx),
        input_variable_info
    )
{}

write_partial::write_partial(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    write_kind kind,
    details::write_primary_target primary,
    std::vector<details::write_secondary_target> secondaries,
    variable_table_info const* input_variable_info
) :
    record_operator(index, info, block_index, input_variable_info),
    kind_(kind),
    primary_(std::move(primary)),
    secondaries_(std::move(secondaries))
{}

details::write_primary_target const& write_partial::primary() const noexcept {
    return primary_;
}

std::vector<details::write_secondary_target> write_partial::create_secondary_targets(
    yugawara::storage::index const& idx
) {
    auto& table = idx.table();
    auto& primary = *table.owner()->find_primary_index(table);
    auto key_meta = index::create_meta(primary, true);
    auto value_meta = index::create_meta(primary, false);
    std::vector<details::write_secondary_target> ret{};
    std::size_t count{};
    table.owner()->each_table_index(table,
        [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
            if (*entry == idx) {
                return;
            }
            ++count;
        }
    );
    ret.reserve(count);
    table.owner()->each_table_index(table,
        [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
            if (*entry == idx) {
                return;
            }
            ret.emplace_back(
                *entry,
                key_meta,
                value_meta
            );
        }
    );
    return ret;
}
}
