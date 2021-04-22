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
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/kvs/coder.h>
#include "operator_base.h"
#include "context_helper.h"
#include "scan_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

scan::scan(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::string_view storage_name,
    std::string_view secondary_storage_name,
    std::vector<details::field_info> key_fields,
    std::vector<details::field_info> value_fields,
    std::vector<details::secondary_index_field_info> secondary_key_fields,
    std::unique_ptr<operator_base> downstream
) :
    record_operator(index, info, block_index),
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
    std::unique_ptr<operator_base> downstream
) :
    scan(
        index,
        info,
        block_index,
        primary_idx.simple_name(),
        secondary_idx != nullptr ? secondary_idx->simple_name() : "",
        create_fields(primary_idx, columns, info, block_index, true),
        create_fields(primary_idx, columns, info, block_index, false),
        create_secondary_key_fields(secondary_idx),
        std::move(downstream)
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
    open(ctx);
    auto target = ctx.variables().store().ref();
    auto resource = ctx.varlen_resource();
    status st{};
    while((st = ctx.it_->next()) == status::ok) {
        utils::checkpoint_holder cp{resource};
        std::string_view k{};
        std::string_view v{};
        if(!ctx.it_->key(k) || !ctx.it_->value(v)) {
            fail();
        }
        if (auto res = field_mapper_(k, v, target, *ctx.stg_, *ctx.tx_, resource); res != status::ok) {
            st = res;
            break;
        }
        if (downstream_) {
            if(auto st2 = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); !st2) {
                ctx.abort();
                close(ctx);
                return {operation_status_kind::aborted};
            }
        }
    }
    if (st == status::not_found) {
        if (downstream_) {
            unsafe_downcast<record_operator>(downstream_.get())->finish(context);
        }
    } else {
        ctx.state(context_state::abort);
        ctx.req_context()->status_code(st);
        return {operation_status_kind::aborted};
    }
    close(ctx);
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

void scan::finish(abstract::task_context*) {
    // top operators decide finish timing on their own
    fail();
}

void scan::open(scan_context& ctx) {
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
    if(auto res = stg.scan(
            *ctx.tx_,
            ctx.scan_info_->begin_key(),
            be,
            ctx.scan_info_->end_key(),
            ee,
            ctx.it_
        );
        res != status::ok) {
        fail();
    }
}

void scan::close(scan_context& ctx) {
    ctx.it_.reset();
}

std::vector<details::field_info> scan::create_fields(
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    processor_info const& info,
    operator_base::block_index_type block_index,
    bool key
) {
    std::vector<details::field_info> ret{};
    using variable = takatori::descriptor::variable;
    yugawara::binding::factory bindings{};
    std::unordered_map<variable, variable> table_to_stream{};
    for(auto&& c : columns) {
        table_to_stream.emplace(c.source(), c.destination());
    }
    auto& block = info.vars_info_list()[block_index];
    if (key) {
        ret.reserve(idx.keys().size());
        for(auto&& k : idx.keys()) {
            auto kc = bindings(k.column());
            auto t = utils::type_for(k.column().type());
            auto spec = k.direction() == relation::sort_direction::ascendant ?
                kvs::spec_key_ascending : kvs::spec_key_descending;
            if (table_to_stream.count(kc) == 0) {
                ret.emplace_back(
                    t,
                    false,
                    0,
                    0,
                    k.column().criteria().nullity().nullable(),
                    spec
                );
                continue;
            }
            auto&& var = table_to_stream.at(kc);
            ret.emplace_back(
                t,
                true,
                block.at(var).value_offset(),
                block.at(var).nullity_offset(),
                k.column().criteria().nullity().nullable(),
                spec
            );
        }
        return ret;
    }
    ret.reserve(idx.values().size());
    for(auto&& v : idx.values()) {
        auto b = bindings(v);
        auto& c = static_cast<yugawara::storage::column const&>(v);
        auto t = utils::type_for(c.type());
        if (table_to_stream.count(b) == 0) {
            ret.emplace_back(
                t,
                false,
                0,
                0,
                c.criteria().nullity().nullable(),
                kvs::spec_value
            );
            continue;
        }
        auto&& var = table_to_stream.at(b);
        ret.emplace_back(
            t,
            true,
            block.at(var).value_offset(),
            block.at(var).nullity_offset(),
            c.criteria().nullity().nullable(),
            kvs::spec_value
        );
    }
    return ret;
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
            kvs::spec_key_ascending : kvs::spec_key_descending;
        ret.emplace_back(
            t,
            k.column().criteria().nullity().nullable(),
            spec
        );
    }
    return ret;
}
}
