/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <chrono>
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
#include <jogasaki/error_code.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/index_field_mapper.h>
#include <jogasaki/executor/process/impl/ops/write_existing.h>
#include <jogasaki/executor/process/impl/scan_info.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/index/field_factory.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/request_cancel_config.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/cancel_request.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/modify_status.h>
#include <jogasaki/utils/set_cancel_status.h>

#include "context_helper.h"
#include "details/encode_key.h"
#include "details/error_abort.h"
#include "operator_base.h"
#include "scan_context.h"

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

operation_status scan::operator()(  //NOLINT(readability-function-cognitive-complexity)
    scan_context& ctx,
    abstract::task_context* context
) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    if(ctx.it_ == nullptr){
        if(auto res = open(ctx); res != status::ok) {
            if(res == status::err_integrity_constraint_violation) {
               // range keys contain null. Nothing should match.
               finish(context);
               return {};
            }
           // res can be status::err_type_mismatch, then ctx already filled with error info
           finish(context);
           return error_abort(ctx, res);
        }
    }
    auto target = ctx.output_variables().store().ref();
    auto resource = ctx.varlen_resource();
    status st{};
    std::size_t loop_count = 0;
    auto scan_block_size = global::config_pool()->scan_block_size();
    auto scan_yield_interval = static_cast<std::int64_t>(global::config_pool()->scan_yield_interval());
    auto previous_time = std::chrono::steady_clock::now();
    while(true) {
        if(utils::request_cancel_enabled(request_cancel_kind::scan) && ctx.req_context()) {
            auto res_src = ctx.req_context()->req_info().response_source();
            if(res_src && res_src->check_cancel()) {
                set_cancel_status(*ctx.req_context());
                ctx.abort();
                finish(context);
                return {operation_status_kind::aborted};
            }
        }
        if((st = ctx.it_->next()) != status::ok) {
            handle_kvs_errors(*ctx.req_context(), st);
            break;
        }
        utils::checkpoint_holder cp{resource};
        std::string_view k{};
        std::string_view v{};
        if((st = ctx.it_->read_key(k)) != status::ok) {
            utils::modify_concurrent_operation_status(*ctx.transaction(), st, true);
            if(st == status::not_found) {
                continue;
            }
            handle_kvs_errors(*ctx.req_context(), st);
            break;
        }
        if((st = ctx.it_->read_value(v)) != status::ok) {
            utils::modify_concurrent_operation_status(*ctx.transaction(), st, true);
            if (st == status::not_found) {
                continue;
            }
            handle_kvs_errors(*ctx.req_context(), st);
            break;
        }
        if (st = field_mapper_(k, v, target, *ctx.stg_, *ctx.tx_, resource); st != status::ok) {
            handle_kvs_errors(*ctx.req_context(), st);
            break;
        }
        if (downstream_) {
            if(auto st2 = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); !st2) {
                ctx.abort();
                finish(context);
                return {operation_status_kind::aborted};
            }
        }
        if (scan_block_size != 0 && scan_block_size <= loop_count ){
            loop_count = 0;
            auto current_time = std::chrono::steady_clock::now();
            auto elapsed_time =
                std::chrono::duration_cast<std::chrono::milliseconds>(current_time - previous_time);
            if (elapsed_time.count() >= scan_yield_interval) {
                ++ctx.yield_count_;
                VLOG_LP(log_trace_fine
                ) << "scan operator yields count:"
                  << ctx.yield_count_ << " loop_count:" << loop_count << " elapsed(us):"
                  << std::chrono::duration_cast<std::chrono::microseconds>(current_time - previous_time).count();

                return {operation_status_kind::yield};
            }
        }
        loop_count++;
    }
    finish(context);
    if (st != status::not_found) {
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
    std::string msg{};
    if(auto res = details::encode_key(
           ctx.req_context(),
           ctx.scan_info_->begin_columns(),
           vars,
           *ctx.varlen_resource(),
           ctx.key_begin_,
           blen,
           msg
       );
       res != status::ok) {
        if(res == status::err_type_mismatch) {
            // only on err_type_mismatch, msg is filled with error message. use it to create the error info in request context
            set_error(*ctx.req_context(), error_code::unsupported_runtime_feature_exception, msg, res);
        }
        return res;
    }
    std::size_t elen{};
    if(auto res = details::encode_key(
           ctx.req_context(),
           ctx.scan_info_->end_columns(),
           vars,
           *ctx.varlen_resource(),
           ctx.key_end_,
           elen,
           msg
       );
       res != status::ok) {
        if(res == status::err_type_mismatch) {
            // only on err_type_mismatch, msg is filled with error message. use it to create the error info in request context
            set_error(*ctx.req_context(), error_code::unsupported_runtime_feature_exception, msg, res);
        }
        return res;
    }
    if(auto res = stg.content_scan(
            *ctx.tx_,
            {static_cast<char*>(ctx.key_begin_.data()), blen},
            be,
            {static_cast<char*>(ctx.key_end_.data()), elen},
            ee,
            ctx.it_
        ); res != status::ok) {
        handle_kvs_errors(*ctx.req_context(), res);
        handle_generic_error(*ctx.req_context(), res, error_code::sql_execution_exception);
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

void scan::dump() const noexcept {
    int width = 28;
    auto downstream_ptr = (downstream_ ? downstream_.get() : nullptr);
    operator_base::dump();
    std::string head = "        ";
    std::cerr << "    record_operator:\n"
       << "      scan:\n"
       << head << std::left << std::setw(width) << "use_secondary_:"
       << std::hex << (use_secondary_ ? "true" : "false") << "\n"
       << head << std::setw(width) << "storage_name_:"
       << storage_name_ << "\n"
       << head << std::setw(width) << "secondary_storage_name_:"
       << secondary_storage_name_ << "\n"
       << head << std::setw(width) << "downstream_:"
       << downstream_ptr <<  std::endl;
    if (downstream_) {
         downstream_ptr->dump("          ");
         std::cerr << "              "
             << to_parent_operator_name(*downstream_) << ":" << std::endl;
    }
    std::cerr << head << std::setw(width) << "field_mapper_:"
       << "not implemented yet" << std::endl;
}
} // namespace jogasaki::executor::process::impl::ops
