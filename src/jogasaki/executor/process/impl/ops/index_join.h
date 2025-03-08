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
#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <takatori/relation/join_find.h>
#include <takatori/relation/join_scan.h>
#include <takatori/scalar/expression.h>
#include <takatori/tree/tree_fragment_vector.h>
#include <takatori/util/optional_ptr.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/storage/index.h>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/context_helper.h>
#include <jogasaki/executor/process/impl/ops/details/error_abort.h>
#include <jogasaki/executor/process/impl/ops/details/expression_error.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/index/field_factory.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/modify_status.h>

#include "details/encode_key.h"
#include "details/search_key_field_info.h"
#include "index_field_mapper.h"
#include "index_join_context.h"
#include "index_matcher.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

namespace details {

/**
 * @brief create secondary index key field info. Kept public for testing
 */
inline std::vector<details::secondary_index_field_info> create_secondary_key_fields(
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

}  // namespace details

/**
 * @brief index_join class common for join_find/join_scan operators
 */
template <class MatchInfo>
class index_join : public record_operator {
public:
    friend class index_join_context<MatchInfo>;

    using join_kind = takatori::relation::join_kind;

    using memory_resource = memory::lifo_paged_memory_resource;

    /**
     * @brief create empty object
     */
    index_join() = default;

    template <class T = MatchInfo, typename = std::enable_if_t<std::is_same_v<T, details::match_info_find>, void>>
    index_join(
        join_kind kind,
        operator_base::operator_index_type index,
        processor_info const& info,
        operator_base::block_index_type block_index,
        std::string_view primary_storage_name,
        std::string_view secondary_storage_name,
        std::vector<index::field_info> key_columns,
        std::vector<index::field_info> value_columns,
        std::vector<details::search_key_field_info> search_key_fields,
        takatori::util::optional_ptr<takatori::scalar::expression const> condition,
        std::vector<details::secondary_index_field_info> secondary_key_fields,
        std::unique_ptr<operator_base> downstream,
        variable_table_info const* input_variable_info = nullptr,
        variable_table_info const* output_variable_info = nullptr
    ) noexcept:
        record_operator(index, info, block_index, input_variable_info, output_variable_info),
        join_kind_(kind),
        for_join_scan_(false),  //NOLINT(modernize-use-default-member-init)
        use_secondary_(! secondary_storage_name.empty()),
        primary_storage_name_(primary_storage_name),
        secondary_storage_name_(secondary_storage_name),
        key_columns_(std::move(key_columns)),
        value_columns_(std::move(value_columns)),
        match_info_(
            std::move(search_key_fields),
            std::move(secondary_key_fields)
        ),
        condition_(std::move(condition)),
        downstream_(std::move(downstream)),
        evaluator_(condition_ ?
            expr::evaluator{*condition_, info.compiled_info(), info.host_variables()} :
            expr::evaluator{}
        )
    {}

    template <class T = MatchInfo, typename = std::enable_if_t<std::is_same_v<T, details::match_info_scan>, void>>
    index_join(
        join_kind kind,
        operator_base::operator_index_type index,
        processor_info const& info,
        operator_base::block_index_type block_index,
        std::string_view primary_storage_name,
        std::string_view secondary_storage_name,
        std::vector<index::field_info> key_columns,
        std::vector<index::field_info> value_columns,
        std::vector<details::search_key_field_info> begin_for_scan,
        kvs::end_point_kind begin_endpoint,
        std::vector<details::search_key_field_info> end_for_scan,
        kvs::end_point_kind end_endpoint,
        takatori::util::optional_ptr<takatori::scalar::expression const> condition,
        std::vector<details::secondary_index_field_info> secondary_key_fields,
        std::unique_ptr<operator_base> downstream,
        variable_table_info const* input_variable_info = nullptr,
        variable_table_info const* output_variable_info = nullptr
    ) noexcept:
        record_operator(index, info, block_index, input_variable_info, output_variable_info),
        join_kind_(kind),
        for_join_scan_(true),
        use_secondary_(! secondary_storage_name.empty()),
        primary_storage_name_(primary_storage_name),
        secondary_storage_name_(secondary_storage_name),
        key_columns_(std::move(key_columns)),
        value_columns_(std::move(value_columns)),
        match_info_(
            std::move(begin_for_scan),
            begin_endpoint,
            std::move(end_for_scan),
            end_endpoint,
            std::move(secondary_key_fields)
        ),
        condition_(std::move(condition)),
        downstream_(std::move(downstream)),
        evaluator_(condition_ ?
            expr::evaluator{*condition_, info.compiled_info(), info.host_variables()} :
            expr::evaluator{}
        )
    {}

    template <class T = MatchInfo, typename = std::enable_if_t<std::is_same_v<T, details::match_info_find>, void>>
    index_join(
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
        variable_table_info const* input_variable_info = nullptr,
        variable_table_info const* output_variable_info = nullptr
    ) :
        index_join(
            kind,
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
            details::create_secondary_key_fields(secondary_idx),
            std::move(downstream),
            input_variable_info,
            output_variable_info
        )
    {}

    template <class T = MatchInfo, typename = std::enable_if_t<std::is_same_v<T, details::match_info_scan>, void>>
    index_join(
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
        variable_table_info const* input_variable_info = nullptr,
        variable_table_info const* output_variable_info = nullptr
    ) :
        index_join(
            kind,
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

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     * @return status of the operation
     */
    operation_status process_record(abstract::task_context* context) override {
        BOOST_ASSERT(context != nullptr);  //NOLINT
        context_helper ctx{*context};
        auto* p = find_context<index_join_context<MatchInfo>>(index(), ctx.contexts());
        if (! p) {
            p = ctx.make_context<index_join_context<MatchInfo>>(
                index(),
                ctx.variable_table(block_index()),
                ctx.variable_table(block_index()),
                ctx.database()->get_storage(primary_storage_name_),
                use_secondary_ ? ctx.database()->get_storage(secondary_storage_name_) : nullptr,
                ctx.transaction(),
                std::make_unique<details::matcher<MatchInfo>>(use_secondary_, match_info_, key_columns_, value_columns_),
                ctx.resource(),
                ctx.varlen_resource()
            );
        }
        return (*this)(*p, context);
    }

    /**
     * @brief process record with context object
     * @details process record, join variables with found result, and invoke downstream when join conditions are met
     * @param ctx operator context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     * @return status of the operation
     */
    operation_status operator()(index_join_context<MatchInfo>& ctx, abstract::task_context* context = nullptr) { //NOLINT(readability-function-cognitive-complexity)
        if (ctx.inactive()) {
            return {operation_status_kind::aborted};
        }
        auto resource = ctx.varlen_resource();
        nullify_output_variables(ctx.output_variables().store().ref());
        bool matched = ctx.matcher_->template process<MatchInfo>(
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
                        ctx.req_context() ? ctx.req_context()->transaction() : nullptr
                    };
                    auto r = evaluate_bool(c, evaluator_, ctx.input_variables(), resource);
                    if (r.error()) {
                        return handle_expression_error(ctx, r, c);
                    }
                    if(! r.template to<bool>()) {
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
            } while (matched && ctx.matcher_->next(*ctx.req_context()));
        }
        // normally `res` is not_found here indicating there are no more records to process
        if(auto res = ctx.matcher_->result(); res != status::ok && res != status::not_found) {
            // on error, error info is already filled in the request context so just finish the operator
            ctx.abort();
            return {operation_status_kind::aborted};
        }
        return {};
    }

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override {
        return std::is_same_v<MatchInfo, details::match_info_find> ? operator_kind::join_find
                                                                   : operator_kind::join_scan;
    }

    /**
     * @brief return storage name
     * @return the storage name of the find target
     */
    [[nodiscard]] std::string_view storage_name() const noexcept {
        return primary_storage_name_;
    }

    /**
     * @brief return match info.
     * @return the match info object
     */
    [[nodiscard]] MatchInfo const& match_info() const noexcept {
        return match_info_;
    }

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context* context) override {
        if (! context) return;
        context_helper ctx{*context};
        if (auto* p = find_context<index_join_context<MatchInfo>>(index(), ctx.contexts())) {
            p->release();
        }
        if (downstream_) {
            unsafe_downcast<record_operator>(downstream_.get())->finish(context);
        }
    }

    /**
     * @brief accessor to key columns
     */
    [[nodiscard]] std::vector<index::field_info> const& key_columns() const noexcept {
        return key_columns_;
    }

    /**
     * @brief accessor to value columns
     */
    [[nodiscard]] std::vector<index::field_info> const& value_columns() const noexcept {
        return value_columns_;
    }

private:
    join_kind join_kind_{};
    bool for_join_scan_{};
    bool use_secondary_{};
    std::string primary_storage_name_{};
    std::string secondary_storage_name_{};
    std::vector<index::field_info> key_columns_{};
    std::vector<index::field_info> value_columns_{};
    MatchInfo match_info_{};
    takatori::util::optional_ptr<takatori::scalar::expression const> condition_{};
    std::unique_ptr<operator_base> downstream_{};
    expr::evaluator evaluator_{};

    void nullify_output_variables(accessor::record_ref target) {
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
};

/**
 * @brief join find operator
 */
using join_find = index_join<details::match_info_find>;

/**
 * @brief join scan operator
 */
using join_scan = index_join<details::match_info_scan>;

/**
 * @brief context object for join_find
 */
using join_find_context = index_join_context<details::match_info_find>;

/**
 * @brief context object for join_scan
 */
using join_scan_context = index_join_context<details::match_info_scan>;

/**
 * @brief matcher object for join_find
 */
using join_find_matcher = details::matcher<details::match_info_find>;

/**
 * @brief matcher object for join_scan
 */
using join_scan_matcher = details::matcher<details::match_info_scan>;

}  // namespace jogasaki::executor::process::impl::ops
