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

#include <cstddef>
#include <utility>
#include <vector>
#include <boost/assert.hpp>

#include <takatori/descriptor/element.h>
#include <takatori/util/downcast.h>
#include <takatori/util/infect_qualifier.h>
#include <takatori/util/reference_iterator.h>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/details/expression_error.h>
#include <jogasaki/executor/process/impl/ops/details/search_key_field_info.h>
#include <jogasaki/executor/process/impl/ops/index_field_mapper.h>
#include <jogasaki/index/field_factory.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/make_function_context.h>
#include <jogasaki/utils/modify_status.h>

#include "context_helper.h"
#include "details/encode_key.h"
#include "details/error_abort.h"
#include "join_find_context.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

namespace details {

std::vector<details::secondary_index_field_info> create_secondary_key_fields(
    yugawara::storage::index const* secondary_idx
) {
    if(secondary_idx == nullptr) {
        return {};
    }
    std::vector<details::secondary_index_field_info> ret{};
    ret.reserve(secondary_idx->keys().size());
    for(auto&& f : secondary_idx->keys()) {
        ret.emplace_back(
            utils::type_for(f.column().type()),
            f.column().criteria().nullity().nullable(),
            f.direction() == takatori::relation::sort_direction::ascendant ? kvs::spec_key_ascending : kvs::spec_key_descending
        );
    }
    return ret;
}

matcher::matcher(
    bool use_secondary,
    bool for_join_scan,
    std::vector<details::search_key_field_info> const& key_fields,
    std::vector<details::search_key_field_info> const& begin_fields,
    kvs::end_point_kind begin_endpoint,
    std::vector<details::search_key_field_info> const& end_fields,
    kvs::end_point_kind end_endpoint,
    std::vector<index::field_info> key_columns,
    std::vector<index::field_info> value_columns,
    std::vector<details::secondary_index_field_info> secondary_key_fields
) :
    use_secondary_(use_secondary),
    for_join_scan_(for_join_scan),
    key_fields_(key_fields),
    begin_fields_(begin_fields),
    begin_endpoint_(begin_endpoint),
    end_fields_(end_fields),
    end_endpoint_(end_endpoint),
    field_mapper_(
        use_secondary_,
        std::move(key_columns),
        std::move(value_columns),
        std::move(secondary_key_fields)
    )
{
    (void) for_join_scan_;
}

matcher::matcher(
    bool use_secondary,
    std::vector<details::search_key_field_info> const& begin_fields,
    kvs::end_point_kind begin_endpoint,
    std::vector<details::search_key_field_info> const& end_fields,
    kvs::end_point_kind end_endpoint,
    std::vector<index::field_info> key_columns,
    std::vector<index::field_info> value_columns,
    std::vector<details::secondary_index_field_info> secondary_key_fields
) :
    matcher(
        use_secondary,
        true,
        {},
        begin_fields,
        begin_endpoint,
        end_fields,
        end_endpoint,
        std::move(key_columns),
        std::move(value_columns),
        std::move(secondary_key_fields)
    )
{}

matcher::matcher(
    bool use_secondary,
    std::vector<details::search_key_field_info> const& key_fields,
    std::vector<index::field_info> key_columns,
    std::vector<index::field_info> value_columns,
    std::vector<details::secondary_index_field_info> secondary_key_fields
) :
    matcher(
        use_secondary,
        false,
        key_fields,
        {},
        {},
        {},
        {},
        std::move(key_columns),
        std::move(value_columns),
        std::move(secondary_key_fields)
    )
{}

bool matcher::process_find(
    request_context& ctx,
    variable_table& input_variables,
    variable_table& output_variables,
    kvs::storage& primary_stg,
    kvs::storage* secondary_stg,
    matcher::memory_resource* resource
) {
    std::size_t len{};
    std::string msg{};
    if(auto res = details::encode_key(std::addressof(ctx), key_fields_, input_variables, *resource, buf_, len, msg);
       res != status::ok) {
        status_ = res;
        // TODO handle msg
        if(res == status::err_integrity_constraint_violation) {
            // null is assigned for find condition. Nothing should match.
            status_ = status::not_found;
        }
        return false;
    }
    std::string_view key{static_cast<char*>(buf_.data()), len};
    std::string_view value{};

    if (! use_secondary_) {
        auto res = primary_stg.content_get(*ctx.transaction(), key, value);
        status_ = res;
        if (res != status::ok) {
            utils::modify_concurrent_operation_status(*ctx.transaction(), res, false);
            status_ = res;
            return false;
        }
        return field_mapper_(key, value, output_variables.store().ref(), primary_stg, *ctx.transaction(), resource) ==
            status::ok;
    }
    // join_find for secondary, or join_scan (for primary or secondary)
    auto& stg = use_secondary_ ? *secondary_stg : primary_stg;
    if(auto res = stg.content_scan(*ctx.transaction(),
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
    tx_ = std::addressof(*ctx.transaction());
    resource_ = resource;
    return next();
}

bool matcher::process_scan(
    request_context& ctx,
    variable_table& input_variables,
    variable_table& output_variables,
    kvs::storage& primary_stg,
    kvs::storage* secondary_stg,
    matcher::memory_resource* resource
) {
    std::size_t begin_len{};
    std::size_t end_len{};
    std::string msg{};
    if(auto res = details::encode_key(std::addressof(ctx), begin_fields_, input_variables, *resource, buf_, begin_len, msg);
    res != status::ok) {
        status_ = res;
        // TODO handle msg
        if (res == status::err_integrity_constraint_violation) {
            // null is assigned for find condition. Nothing should match.
            status_ = status::not_found;
        }
        return false;
    }
    if(auto res = details::encode_key(std::addressof(ctx), end_fields_, input_variables, *resource, buf2_, end_len, msg);
    res != status::ok) {
        status_ = res;
        // TODO handle msg
        if (res == status::err_integrity_constraint_violation) {
            // null is assigned for find condition. Nothing should match.
            status_ = status::not_found;
        }
        return false;
    }
    std::string_view b{static_cast<char*>(buf_.data()), begin_len};
    std::string_view e{static_cast<char*>(buf2_.data()), end_len};

    auto& stg = use_secondary_ ? *secondary_stg : primary_stg;
    if(auto res = stg.content_scan(*ctx.transaction(),
            b, begin_endpoint_,
            e, end_endpoint_,
            it_
        ); res != status::ok) {
            status_ = res;
            return false;
    }

    // remember parameters for current scan
    output_variables_ = std::addressof(output_variables);
    primary_storage_ = std::addressof(primary_stg);
    tx_ = std::addressof(*ctx.transaction());
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
        if(auto r = it_->read_key(key); r != status::ok) {
            utils::modify_concurrent_operation_status(*tx_, r, true);
            if(r == status::not_found) {
                continue;
            }
            status_ = r;
            it_.reset();
            return false;
        }
        if(auto r = it_->read_value(value); r != status::ok) {
            utils::modify_concurrent_operation_status(*tx_, r, true);
            if(r == status::not_found) {
                continue;
            }
            status_ = r;
            it_.reset();
            return false;
        }
        return field_mapper_(key, value, output_variables_->store().ref(), *primary_storage_, *tx_, resource_) ==
            status::ok;
    }
}

status matcher::result() const noexcept {
    return status_;
}

}  // namespace details

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
            for_join_scan_ ? std::make_unique<details::matcher>(
                                 use_secondary_,
                                 begin_for_scan_,
                                 begin_endpoint_,
                                 end_for_scan_,
                                 end_endpoint_,
                                 key_columns_,
                                 value_columns_,
                                 secondary_key_fields_
                             )
                           : std::make_unique<details::matcher>(
                                 use_secondary_,
                                 search_key_fields_,
                                 key_columns_,
                                 value_columns_,
                                 secondary_key_fields_
                             ),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

void join_find::nullify_output_variables(accessor::record_ref target) {
    for(auto&& f : key_columns_) {
        if(f.exists_) {
            target.set_null(f.nullity_offset_, true);
        }
    }
    for(auto&& f : value_columns_) {
        if(f.exists_) {
            target.set_null(f.nullity_offset_, true);
        }
    }
}
operation_status join_find::operator()(join_find_context& ctx, abstract::task_context* context) {  //NOLINT(readability-function-cognitive-complexity)
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    auto resource = ctx.varlen_resource();
    nullify_output_variables(ctx.output_variables().store().ref());
    bool matched = for_join_scan_ ? ctx.matcher_->process_scan(
                                        *ctx.req_context(),
                                        ctx.input_variables(),
                                        ctx.output_variables(),
                                        *ctx.primary_stg_,
                                        ctx.secondary_stg_.get(),
                                        resource
                                    )
                                  : ctx.matcher_->process_find(
                                        *ctx.req_context(),
                                        ctx.input_variables(),
                                        ctx.output_variables(),
                                        *ctx.primary_stg_,
                                        ctx.secondary_stg_.get(),
                                        resource
                                    );
    if(matched || join_kind_ == join_kind::left_outer) {
        do {
            if (condition_) {
                expr::evaluator_context c{
                    resource,
                    ctx.req_context() ? utils::make_function_context(*ctx.req_context()->transaction()) : nullptr
                };
                auto r = evaluate_bool(c, evaluator_, ctx.input_variables(), resource);
                if (r.error()) {
                    return handle_expression_error(ctx, r, c);
                }
                if(! r.to<bool>()) {
                    if(join_kind_ != join_kind::left_outer) {
                        // inner join: skip record
                        continue;
                    }
                    // left outer join: nullify output variables and send record downstream
                    nullify_output_variables(ctx.output_variables().store().ref());
                }
            }
            if (downstream_) {
                if(auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); !st) {
                    ctx.abort();
                    return {operation_status_kind::aborted};
                }
            }
            // clean output variables for next record just in case
            nullify_output_variables(ctx.output_variables().store().ref());
        } while(matched && ctx.matcher_->next());
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
    join_kind kind,
    bool for_join_scan,
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::string_view primary_storage_name,
    std::string_view secondary_storage_name,
    std::vector<index::field_info> key_columns,
    std::vector<index::field_info> value_columns,
    std::vector<details::search_key_field_info> search_key_fields,
    std::vector<details::search_key_field_info> begin_for_scan,
    kvs::end_point_kind begin_endpoint,
    std::vector<details::search_key_field_info> end_for_scan,
    kvs::end_point_kind end_endpoint,
    takatori::util::optional_ptr<takatori::scalar::expression const> condition,
    std::vector<details::secondary_index_field_info> secondary_key_fields,
    std::unique_ptr<operator_base> downstream,
    variable_table_info const* input_variable_info,
    variable_table_info const* output_variable_info
) noexcept:
    record_operator(index, info, block_index, input_variable_info, output_variable_info),
    join_kind_(kind),
    for_join_scan_(for_join_scan),
    use_secondary_(! secondary_storage_name.empty()),
    primary_storage_name_(primary_storage_name),
    secondary_storage_name_(secondary_storage_name),
    key_columns_(std::move(key_columns)),
    value_columns_(std::move(value_columns)),
    search_key_fields_(std::move(search_key_fields)),
    begin_for_scan_(std::move(begin_for_scan)),
    begin_endpoint_(begin_endpoint),
    end_for_scan_(std::move(end_for_scan)),
    end_endpoint_(end_endpoint),
    condition_(std::move(condition)),
    downstream_(std::move(downstream)),
    evaluator_(condition_ ?
        expr::evaluator{*condition_, info.compiled_info(), info.host_variables()} :
        expr::evaluator{}
    ),
    secondary_key_fields_(std::move(secondary_key_fields))
{}

join_find::join_find(
    join_kind kind,
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    yugawara::storage::index const& primary_idx,
    sequence_view<takatori::relation::join_find::column const> columns,
    takatori::tree::tree_fragment_vector<takatori::relation::join_find::key> const& keys,
    takatori::util::optional_ptr<takatori::scalar::expression const> condition,
    yugawara::storage::index const* secondary_idx,
    std::unique_ptr<operator_base> downstream,
    variable_table_info const* input_variable_info,
    variable_table_info const* output_variable_info
) :
    join_find(
        kind,
        false,  // for_join_scan
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
        {},
        {},
        {},
        {},
        condition,
        details::create_secondary_key_fields(secondary_idx),
        std::move(downstream),
        input_variable_info,
        output_variable_info
    )
{}

join_find::join_find(
    join_kind kind,
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    yugawara::storage::index const& primary_idx,
    sequence_view<takatori::relation::join_find::column const> columns,
    takatori::tree::tree_fragment_vector<takatori::relation::join_scan::key> const& begin_for_scan,
    kvs::end_point_kind begin_endpoint,
    takatori::tree::tree_fragment_vector<takatori::relation::join_scan::key> const& end_for_scan,
    kvs::end_point_kind end_endpoint,
    takatori::util::optional_ptr<takatori::scalar::expression const> condition,
    yugawara::storage::index const* secondary_idx,
    std::unique_ptr<operator_base> downstream,
    variable_table_info const* input_variable_info,
    variable_table_info const* output_variable_info
) :
    join_find(
        kind,
        true,  // for_join_scan
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
        {},
        details::create_search_key_fields(
            secondary_idx != nullptr ? *secondary_idx : primary_idx,
            begin_for_scan,
            info
        ),
        begin_endpoint,
        details::create_search_key_fields(
            secondary_idx != nullptr ? *secondary_idx : primary_idx,
            end_for_scan,
            info
        ),
        end_endpoint,
        condition,
        details::create_secondary_key_fields(secondary_idx),
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

}  // namespace jogasaki::executor::process::impl::ops
