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
#include "join_find.h"

#include <vector>

#include <takatori/util/downcast.h>

#include <takatori/relation/join_find.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/index/field_factory.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/details/expression_error.h>
#include "operator_base.h"
#include "context_helper.h"
#include "join_find_context.h"
#include "details/encode_key.h"
#include "details/error_abort.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

namespace details {

std::vector<details::secondary_index_field_info> create_secondary_key_fields(
    std::vector<details::search_key_field_info> const& key_fields
) {
    std::vector<details::secondary_index_field_info> ret{};
    ret.reserve(key_fields.size());
    for(auto&& f : key_fields) {
        ret.emplace_back(
            f.type_,
            f.nullable_,
            f.spec_
        );
    }
    return ret;
}

matcher::matcher(
    bool use_secondary,
    std::vector<details::search_key_field_info> const& key_fields,
    std::vector<index::field_info> key_columns,
    std::vector<index::field_info> value_columns
) :
    use_secondary_(use_secondary),
    key_fields_(key_fields),
    field_mapper_(
        use_secondary_,
        std::move(key_columns),
        std::move(value_columns),
        create_secondary_key_fields(key_fields_)
    )
{}

bool matcher::operator()(
    variable_table& input_variables,
    variable_table& output_variables,
    kvs::storage& primary_stg,
    kvs::storage* secondary_stg,
    transaction_context& tx,
    matcher::memory_resource* resource
) {
    std::size_t len{};
    if(auto res = details::encode_key(key_fields_, input_variables, *resource, buf_, len); res != status::ok) {
        status_ = res;
        return false;
    }
    std::string_view key{static_cast<char*>(buf_.data()), len};
    std::string_view value{};

    if (! use_secondary_) {
        auto res = primary_stg.get(tx, key, value);
        status_ = res;
        if (res != status::ok) {
            return false;
        }
        return field_mapper_(key, value, output_variables.store().ref(), primary_stg, tx, resource) == status::ok;
    }
    auto& stg = *secondary_stg;
    if(auto res = stg.scan(tx,
            key, kvs::end_point_kind::prefixed_inclusive,
            key, kvs::end_point_kind::prefixed_inclusive,
            it_
        ); res != status::ok) {
            status_ = res;
            return false;
    }

    // remember parameters for current scan
    output_variables_ = std::addressof(output_variables);
    primary_storage_ = std::addressof(primary_stg);
    tx_ = std::addressof(tx);
    resource_ = resource;
    return next();
}

bool matcher::next() {
    if (it_ == nullptr) {
        status_ = status::not_found;
        return false;
    }
    while(true) {  // loop to skip not_found with key()/value()
        auto res = it_->next();
        if(res != status::ok) {
            status_ = res;
            it_.reset();
            return false;
        }
        std::string_view key{};
        std::string_view value{};
        if(auto r = it_->key(key); r != status::ok) {
            if(r == status::not_found) {
                continue;
            }
            status_ = r;
            it_.reset();
            return false;
        }
        if(auto r = it_->value(value); r != status::ok) {
            if(r == status::not_found) {
                continue;
            }
            status_ = r;
            it_.reset();
            return false;
        }
        return field_mapper_(key, value, output_variables_->store().ref(), *primary_storage_, *tx_, resource_) == status::ok;
    }
}

status matcher::result() const noexcept {
    return status_;
}

}

operation_status join_find::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<join_find_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<join_find_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.variable_table(block_index()),
            ctx.database()->get_storage(primary_storage_name_),
            use_secondary_ ? ctx.database()->get_storage(secondary_storage_name_) : nullptr,
            ctx.transaction(),
            std::make_unique<details::matcher>(
                use_secondary_,
                search_key_fields_,
                key_columns_,
                value_columns_
            ),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status join_find::operator()(join_find_context& ctx, abstract::task_context* context) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    auto resource = ctx.varlen_resource();
    if((*ctx.matcher_)(
        ctx.input_variables(),
        ctx.output_variables(),
        *ctx.primary_stg_,
        ctx.secondary_stg_.get(),
        *ctx.tx_,
        resource
    )) {
        do {
            if (condition_) {
                expression::evaluator_context c{};
                auto r = evaluate_bool(c, evaluator_, ctx.input_variables(), resource);
                if (r.error()) {
                    return handle_expression_error(ctx, r);
                }
                if(! r.to<bool>()) {
                    continue;
                }
            }
            if (downstream_) {
                if(auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); !st) {
                    ctx.abort();
                    return {operation_status_kind::aborted};
                }
            }
        } while(ctx.matcher_->next());
    }
    if(auto res = ctx.matcher_->result(); res != status::not_found) {
        if(res == status::err_integrity_constraint_violation) {
            // match condition saw null. No record should match.
            return {};
        }
        handle_kvs_errors(*ctx.req_context(), res);
        return error_abort(ctx, res);
    }
    return {};
}

operator_kind join_find::kind() const noexcept {
    return operator_kind::join_find;
}

std::string_view join_find::storage_name() const noexcept {
    return primary_storage_name_;
}

void join_find::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if (auto* p = find_context<join_find_context>(index(), ctx.contexts())) {
        p->release();
    }
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

join_find::join_find(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::string_view primary_storage_name,
    std::string_view secondary_storage_name,
    std::vector<index::field_info> key_columns,
    std::vector<index::field_info> value_columns,
    std::vector<details::search_key_field_info> search_key_fields,
    takatori::util::optional_ptr<takatori::scalar::expression const> condition,
    std::unique_ptr<operator_base> downstream,
    variable_table_info const* input_variable_info,
    variable_table_info const* output_variable_info
) noexcept:
    record_operator(index, info, block_index, input_variable_info, output_variable_info),
    use_secondary_(! secondary_storage_name.empty()),
    primary_storage_name_(primary_storage_name),
    secondary_storage_name_(secondary_storage_name),
    key_columns_(std::move(key_columns)),
    value_columns_(std::move(value_columns)),
    search_key_fields_(std::move(search_key_fields)),
    condition_(std::move(condition)),
    downstream_(std::move(downstream)),
    evaluator_(condition_ ?
        expression::evaluator{*condition_, info.compiled_info(), info.host_variables()} :
        expression::evaluator{}
    )
{}

join_find::join_find(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    yugawara::storage::index const& primary_idx,
    sequence_view<column const> columns,
    takatori::tree::tree_fragment_vector<key> const& keys,
    takatori::util::optional_ptr<takatori::scalar::expression const> condition,
    yugawara::storage::index const* secondary_idx,
    std::unique_ptr<operator_base> downstream,
    variable_table_info const* input_variable_info,
    variable_table_info const* output_variable_info
) :
    join_find(
        index,
        info,
        block_index,
        primary_idx.simple_name(),
        secondary_idx != nullptr ? secondary_idx->simple_name() : "",
        index::create_fields(
            primary_idx,
            columns,
            (output_variable_info != nullptr ? *output_variable_info : info.vars_info_list()[block_index]),
            true,
            true
        ),
        index::create_fields(
            primary_idx,
            columns,
            (output_variable_info != nullptr ? *output_variable_info : info.vars_info_list()[block_index]),
            false,
            true
        ),
        details::create_search_key_fields(
            secondary_idx != nullptr ? *secondary_idx : primary_idx,
            keys,
            info
        ),
        condition,
        std::move(downstream),
        input_variable_info,
        output_variable_info
    )
{}

std::vector<index::field_info> const& join_find::key_columns() const noexcept {
    return key_columns_;
}

std::vector<index::field_info> const& join_find::value_columns() const noexcept {
    return value_columns_;
}

std::vector<details::search_key_field_info> const& join_find::search_key_fields() const noexcept {
    return search_key_fields_;
}


}


