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
#include "scan.h"

#include <vector>

#include <takatori/util/downcast.h>
#include <takatori/relation/scan.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/index/field_factory.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/handle_errors.h>
#include <jogasaki/kvs/coder.h>
#include "operator_base.h"
#include "context_helper.h"
#include "scan_context.h"
#include "operator_builder.h"
#include "details/encode_key.h"
#include "details/error_abort.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

scan::scan(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::string_view storage_name,
    std::string_view secondary_storage_name,
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
    downstream_(std::move(downstream)),
    field_mapper_(
        use_secondary_,
        std::move(key_fields),
        std::move(value_fields),
        std::move(secondary_key_fields)
    )
{}

scan::scan(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    yugawara::storage::index const& primary_idx,
    sequence_view<column const> columns,
    yugawara::storage::index const* secondary_idx,
    std::unique_ptr<operator_base> downstream,
    variable_table_info const* input_variable_info,
    variable_table_info const* output_variable_info
) :
    scan(
        index,
        info,
        block_index,
        primary_idx.simple_name(),
        secondary_idx != nullptr ? secondary_idx->simple_name() : "",
        index::create_fields(primary_idx, columns, (output_variable_info != nullptr ? *output_variable_info : info.vars_info_list()[block_index]), true, true),
        index::create_fields(primary_idx, columns, (output_variable_info != nullptr ? *output_variable_info : info.vars_info_list()[block_index]), false, true),
        create_secondary_key_fields(secondary_idx),
        std::move(downstream),
        input_variable_info,
        output_variable_info
    )
{}

operation_status scan::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<scan_context>(index(), ctx.contexts());
    auto stg = ctx.database()->get_storage(storage_name());
    BOOST_ASSERT(stg);  //NOLINT //TODO handle error
    if (! p) {
        p = ctx.make_context<scan_context>(index(),
            ctx.variable_table(block_index()),
            std::move(stg),
            use_secondary_ ? ctx.database()->get_storage(secondary_storage_name()) : nullptr,
            ctx.transaction(),
            unsafe_downcast<impl::scan_info const>(ctx.task_context()->scan_info()),  //NOLINT
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status scan::operator()(scan_context& ctx, abstract::task_context* context) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    if(auto res = open(ctx); res != status::ok) {
        if(res == status::err_integrity_constraint_violation) {
            // range keys contain null. Nothing should match.
            finish(context);
            return {};
        }
        finish(context);
        return error_abort(ctx, res);
    }
    auto target = ctx.output_variables().store().ref();
    auto resource = ctx.varlen_resource();
    status st{};
    while((st = ctx.it_->next()) == status::ok) {
        utils::checkpoint_holder cp{resource};
        std::string_view k{};
        std::string_view v{};
        if((st = ctx.it_->key(k)) != status::ok) {
            if (st == status::not_found) {
                continue;
            }
            break;
        }
        if((st = ctx.it_->value(v)) != status::ok) {
            if (st == status::not_found) {
                continue;
            }
            break;
        }
        if (st = field_mapper_(k, v, target, *ctx.stg_, *ctx.tx_, resource); st != status::ok) {
            break;
        }
        if (downstream_) {
            if(auto st2 = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); !st2) {
                ctx.abort();
                finish(context);
                return {operation_status_kind::aborted};
            }
        }
    }
    finish(context);
    if (st != status::not_found) {
        handle_errors(*ctx.req_context(), st);  // for kvs error
        return error_abort(ctx, st);
    }
    return {};
}

operator_kind scan::kind() const noexcept {
    return operator_kind::scan;
}

std::string_view scan::storage_name() const noexcept {
    return storage_name_;
}

std::string_view scan::secondary_storage_name() const noexcept {
    return secondary_storage_name_;
}

void scan::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if(auto* p = find_context<scan_context>(index(), ctx.contexts())) {
        close(*p);
        p->release();
    }
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

status scan::open(scan_context& ctx) {  //NOLINT(readability-make-member-function-const)
    auto& stg = use_secondary_ ? *ctx.secondary_stg_ : *ctx.stg_;
    auto be = ctx.scan_info_->begin_endpoint();
    auto ee = ctx.scan_info_->end_endpoint();
    if (use_secondary_) {
        // at storage layer, secondary index key contains primary key index as postfix
        // so boundary condition needs promotion to be compatible
        // TODO verify the promotion
        if (be == kvs::end_point_kind::inclusive) {
            be = kvs::end_point_kind::prefixed_inclusive;
        }
        if (be == kvs::end_point_kind::exclusive) {
            be = kvs::end_point_kind::prefixed_exclusive;
        }
        if (ee == kvs::end_point_kind::inclusive) {
            ee = kvs::end_point_kind::prefixed_inclusive;
        }
        if (ee == kvs::end_point_kind::exclusive) {
            ee = kvs::end_point_kind::prefixed_exclusive;
        }
    }
    executor::process::impl::variable_table vars{};
    std::size_t blen{};
    if(auto res = details::encode_key(ctx.scan_info_->begin_columns(), vars, *ctx.varlen_resource(), ctx.key_begin_, blen); res != status::ok) {
        return res;
    }
    std::size_t elen{};
    if(auto res = details::encode_key(ctx.scan_info_->end_columns(), vars, *ctx.varlen_resource(), ctx.key_end_, elen); res != status::ok) {
        return res;
    }
    if(auto res = stg.scan(
            *ctx.tx_,
            {static_cast<char*>(ctx.key_begin_.data()), blen},
            be,
            {static_cast<char*>(ctx.key_end_.data()), elen},
            ee,
            ctx.it_
        ); res != status::ok) {
        handle_errors(*ctx.req_context(), res);
        return res;
    }
    return status::ok;
}

void scan::close(scan_context& ctx) {
    ctx.it_.reset();
}

std::vector<details::secondary_index_field_info> scan::create_secondary_key_fields(
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
}
