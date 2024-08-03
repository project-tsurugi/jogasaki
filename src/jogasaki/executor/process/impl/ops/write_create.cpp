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
#include "write_create.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <boost/assert.hpp>

#include <takatori/descriptor/element.h>
#include <takatori/descriptor/variable.h>
#include <takatori/relation/write.h>
// #include <takatori/util/exception.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/optional_ptr.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/configuration.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/conv/assignment.h>
#include <jogasaki/executor/process/impl/expression/details/cast_evaluation.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/ops/write_create_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/index/primary_context.h>
#include <jogasaki/index/primary_target.h>
#include <jogasaki/index/secondary_context.h>
#include <jogasaki/index/secondary_target.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/field_types.h>

#include "context_helper.h"
#include "details/error_abort.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

using variable = takatori::descriptor::variable;
// using takatori::util::throw_exception;

namespace details {

std::vector<index::secondary_target> create_secondary_targets(
    yugawara::storage::index const& idx,
    sequence_view<write_create::column const> columns
) {
    (void) columns;
    auto& table = idx.table();
    auto& primary = *table.owner()->find_primary_index(table);
    auto key_meta = index::create_meta(primary, true);
    auto value_meta = index::create_meta(primary, false);
    std::vector<index::secondary_target> ret{};
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

}  // namespace details

void write_create::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if(auto* p = find_context<write_create_context>(index(), ctx.contexts())) {
        p->release();
    }
}

std::string_view write_create::storage_name() const noexcept {
    return entity_->primary().storage_name();
}

operator_kind write_create::kind() const noexcept {
    return operator_kind::write_create;
}

operation_status write_create::operator()(write_create_context& ctx) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }

    insert::write_context wctx(
        *ctx.req_context(),
        storage_name(),
        entity_->primary().key_meta(),
        entity_->primary().value_meta(),
        entity_->secondaries(),
        *ctx.req_context()->database(),
        ctx.varlen_resource()
    );  // currently common::write uses the same resource for building mirror and executing runtime


    if(! entity_->process_record(*ctx.req_context(), wctx)) {
        return {operation_status_kind::aborted};
    }
    return {};
}

/*
void abort_transaction(transaction_context& tx) {
    if (auto res = tx.abort(); res != status::ok) {
        throw_exception(std::logic_error{"abort failed unexpectedly"});
    }
}

operation_status write_create::do_update(write_create_context& ctx) {
    auto& context = ctx.primary_context();
    // find update target and fill internal extracted key/values in primary target
    std::string_view encoded{};
    if(auto res = primary_.encode_find(
            context,
            *ctx.transaction(),
            ctx.input_variables().store().ref(),
            ctx.varlen_resource(),
            context.extracted_key(),
            context.extracted_value(),
            encoded
        ); res != status::ok) {
        abort_transaction(*ctx.transaction());
        return error_abort(ctx, res);
    }

    // if(primary_key_updated_ || ! update_skips_deletion(ctx)) {
        // remove and recreate records
        if(auto res = primary_.remove_by_encoded_key(
                context,
                *ctx.transaction(),
                encoded
            ); res != status::ok) {
            abort_transaction(*ctx.transaction());
            return error_abort(ctx, res);
        }
    // }

    for(std::size_t i=0, n=secondaries_.size(); i<n; ++i) {
        // if(! primary_key_updated_ && ! secondary_key_updated_[i] && update_skips_deletion(ctx)) {
        //     continue;
        // }
        if(auto res = secondaries_[i].encode_remove(
            ctx.secondary_contexts_[i],
            *ctx.transaction(),
            context.extracted_key(),
            context.extracted_value(),
            context.encoded_key()
        ); res != status::ok) {
            abort_transaction(*ctx.transaction());
            return error_abort(ctx, res);
        }
    }

    // encode extracted key/value in primary target and send to kvs
    kvs::put_option opt = kind_ == write_kind::insert_overwrite ? kvs::put_option::create_or_update : kvs::put_option::create ;
    std::string_view encoded_key{};
    if(auto res = primary_.encode_put(
           context,
           *ctx.transaction(),
           opt,
           context.extracted_key(),
           context.extracted_value(),
           encoded_key
       );
       res != status::ok) {
        abort_transaction(*ctx.transaction());
        if(res == status::already_exists) {
            res = status::err_unique_constraint_violation;
        }
        return error_abort(ctx, res);
    }
    if(context.req_context()) {
        context.req_context()->enable_stats()->counter(counter_kind::updated).count(1);
    }

    for(std::size_t i=0, n=secondaries_.size(); i<n; ++i) {
        // if(! primary_key_updated_ && ! secondary_key_updated_[i] && update_skips_deletion(ctx)) {
        //     continue;
        // }
        if(auto res = secondaries_[i].encode_put(
                ctx.secondary_contexts_[i],
                *ctx.transaction(),
                context.extracted_key(),
                context.extracted_value(),
                context.encoded_key()
            ); res != status::ok) {
            abort_transaction(*ctx.transaction());
            return error_abort(ctx, res);
        }
    }
    return {};
}

operation_status write_create::do_delete(write_create_context& ctx) {
    auto& context = ctx.primary_context();
    if(secondaries_.empty()) {
        if(auto res = primary_.encode_remove(
                context,
                *ctx.transaction(),
                ctx.input_variables().store().ref()
            ); res != status::ok) {
            return error_abort(ctx, res);
        }
        if(context.req_context()) {
            context.req_context()->enable_stats()->counter(counter_kind::deleted).count(1);
        }
        return {};
    }

    if(auto res = primary_.encode_find_remove(
            context,
            *ctx.transaction(),
            ctx.input_variables().store().ref(),
            ctx.varlen_resource(),
            context.extracted_key(),
            context.extracted_value()
        ); res != status::ok) {
        return error_abort(ctx, res);
    }
    if(context.req_context()) {
        context.req_context()->enable_stats()->counter(counter_kind::deleted).count(1);
    }

    for(std::size_t i=0, n=secondaries_.size(); i<n; ++i) {
        if(auto res = secondaries_[i].encode_remove(
                ctx.secondary_contexts_[i],
                *ctx.transaction(),
                context.extracted_key(),
                context.extracted_value(),
                context.encoded_key()
            ); res != status::ok) {
            return error_abort(ctx, res);
        }
    }
    return {};
}
*/

operation_status write_create::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<write_create_context>(index(), ctx.contexts());
    if (! p) {
        std::vector<index::secondary_context> contexts{};
        contexts.reserve(entity_->secondaries().size());
        for(auto&& s : entity_->secondaries()) {
            contexts.emplace_back(
                ctx.database()->get_or_create_storage(s.storage_name()),
                ctx.req_context()
            );
        }
        p = ctx.make_context<write_create_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.database()->get_storage(storage_name()),
            ctx.transaction(),
            entity_->primary().key_meta(),
            entity_->primary().value_meta(),
            ctx.resource(),
            ctx.varlen_resource(),
            std::move(contexts)
        );
    }
    return (*this)(*p);
}

write_create::write_create(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    write_kind kind,
    yugawara::storage::index const& idx,
    sequence_view<key const> keys,
    sequence_view<column const> columns,
    variable_table_info const* input_variable_info
) :
    write_create(
        index,
        info,
        block_index,
        kind,
        index::primary_target{
            idx,
            keys,
            input_variable_info ? *input_variable_info : info.vars_info_list()[block_index]
        },
        details::create_secondary_targets(idx, columns),
        input_variable_info
    )
{}

write_create::write_create(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    write_kind kind,
    index::primary_target primary,
    std::vector<index::secondary_target> secondaries,
    variable_table_info const* input_variable_info
) :
    record_operator(index, info, block_index, input_variable_info),
    kind_(kind),
    entity_(std::make_shared<insert::insert_new_record>(kind_, std::move(primary), std::move(secondaries))) {}

index::primary_target const& write_create::primary() const noexcept {
    return entity_->primary();
}

}  // namespace jogasaki::executor::process::impl::ops
