/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/descriptor/variable.h>
#include <takatori/relation/endpoint_kind.h>
#include <takatori/relation/join_find.h>
#include <takatori/relation/join_kind.h>
#include <takatori/relation/join_scan.h>
#include <takatori/relation/sort_direction.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/expression.h>
#include <takatori/tree/tree_fragment_vector.h>
#include <takatori/type/primitive.h>
#include <takatori/util/optional_ptr.h>
#include <takatori/util/sequence_view.h>

#include <yugawara/analyzer/expression_resolution.h>
#include <yugawara/analyzer/variable_resolution.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/executor/process/impl/ops/index_join.h>
#include <jogasaki/executor/process/impl/ops/index_join_context.h>
#include <jogasaki/executor/process/impl/ops/index_matcher.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs_test_base.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/operator_test_utils.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/from_endpoint.h>

namespace jogasaki::executor::process::impl::ops {

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;
namespace t = ::takatori::type;

using takatori::util::optional_ptr;
using takatori::util::sequence_view;

/**
 * @brief Bundle of runtime objects needed to invoke the join_scan operator.
 * @details join_scan is a passive record operator: upstream records supply
 *     KVS range-scan bounds; the operator scans the range and writes results
 *     back into the single combined variable block shared by all stream variables.
 *     match_info_ is a member (not a local variable) so that the join_scan_matcher
 *     stored in ctx_ holds a valid reference throughout the executor's lifetime.
 *     ctx_ is emplaced in the constructor body after op_ and match_info_ are
 *     fully initialised.  The struct must not be copy- or move-constructed.
 *     Always create via join_scan_test_base::make_join_scan_executor().
 */
struct join_scan_executor {
    join_scan op_;
    details::match_info_scan match_info_;
    variable_table_list variables_list_;
    mock::task_context task_ctx_;
    std::optional<join_scan_context> ctx_;

    join_scan_executor(
        processor_info const& pinfo,
        relation::join_kind kind,
        yugawara::storage::index const& primary_idx,
        sequence_view<relation::join_find::column const> columns,
        takatori::tree::tree_fragment_vector<relation::join_scan::key> const& begin_keys,
        kvs::end_point_kind begin_endpoint,
        takatori::tree::tree_fragment_vector<relation::join_scan::key> const& end_keys,
        kvs::end_point_kind end_endpoint,
        optional_ptr<scalar::expression const> condition,
        yugawara::storage::index const* secondary_idx,
        std::unique_ptr<operator_base> downstream,
        std::unique_ptr<kvs::storage> primary_stg,
        std::unique_ptr<kvs::storage> secondary_stg,
        transaction_context* tx_raw,
        std::shared_ptr<transaction_context> tx_shared,
        memory::lifo_paged_memory_resource* res,
        memory::lifo_paged_memory_resource* varlen_res,
        request_context* req_ctx
    ) :
        op_{kind, 0, pinfo, 0, primary_idx, columns,
            begin_keys, begin_endpoint, end_keys, end_endpoint,
            condition, secondary_idx, std::move(downstream)},
        match_info_{op_.match_info().begin_fields_,
            op_.match_info().begin_endpoint_,
            op_.match_info().end_fields_,
            op_.match_info().end_endpoint_,
            details::create_secondary_key_fields(secondary_idx)},
        variables_list_{},
        task_ctx_{{}, {}, {}, {}}
    {
        variables_list_.emplace_back(pinfo.vars_info_list()[op_.block_index()]);
        ctx_.emplace(
            &task_ctx_,
            variables_view{variables_list_, 0},
            std::move(primary_stg),
            std::move(secondary_stg),
            tx_raw,
            std::make_unique<join_scan_matcher>(
                secondary_idx != nullptr, match_info_, op_.key_columns(), op_.value_columns()),
            res,
            varlen_res,
            nullptr
        );
        ctx_->task_context().work_context(std::make_unique<impl::work_context>(
            req_ctx, 0, op_.block_index(), nullptr, nullptr, nullptr, tx_shared, false, false
        ));
    }

    join_scan_executor(join_scan_executor const&) = delete;
    join_scan_executor& operator=(join_scan_executor const&) = delete;
    join_scan_executor(join_scan_executor&&) = delete;
    join_scan_executor& operator=(join_scan_executor&&) = delete;
};

/**
 * @brief Shared test fixture base for join_scan operator tests.
 *
 * @details Provides the join_scan_executor bundle, SetUp/TearDown lifecycle,
 *     add_join_scan_node helpers, make_join_scan_executor helpers, and
 *     scan_endpoint_spec.  Derive your GTest fixture from this class rather
 *     than replicating the infrastructure in each test file.
 */
class join_scan_test_base :
    public test_root,
    public kvs_test_base,
    public operator_test_utils {
public:
    static constexpr auto asc = relation::sort_direction::ascendant;
    static constexpr auto desc = relation::sort_direction::descendant;

    void SetUp() override {
        kvs_db_setup();
    }
    void TearDown() override {
        kvs_db_teardown();
    }

    /**
     * @brief Specifies one endpoint (lower or upper) of a join_scan range.
     * @details col_indices and source_vars must be the same length.
     *     An unbound endpoint has empty col_indices and kind=unbound.
     */
    struct scan_endpoint_spec {
        std::vector<std::size_t> col_indices{};
        std::vector<descriptor::variable> source_vars{};
        relation::endpoint_kind kind{relation::endpoint_kind::unbound};

        static scan_endpoint_spec unbound() {
            return {};
        }
    };

    using condition_factory_t =
        std::function<std::unique_ptr<scalar::expression>(std::vector<descriptor::variable> const&)>;

    /**
     * @brief Insert a join_scan relation node into the process graph.
     *
     * @details All table columns are mapped to fresh stream variables as output
     *     columns.  Column and key-expression types are derived from the table
     *     metadata.  If condition_factory is provided it is called with the
     *     column destination variables so the condition can reference them; the
     *     returned expression is bound assuming a top-level int4 compare.
     *
     * @param setup              table and index configuration
     * @param lower              lower endpoint specification
     * @param upper              upper endpoint specification
     * @param use_secondary      if true, targets setup.secondary_idx; otherwise
     *                           setup.primary_idx
     * @param condition_factory  optional factory called with column destinations
     *                           to produce a condition expression (int4 compare)
     * @return reference to the newly inserted node
     */
    relation::join_scan& add_join_scan_node(
        table_setup const& setup,
        scan_endpoint_spec const& lower,
        scan_endpoint_spec const& upper,
        bool use_secondary = false,
        condition_factory_t condition_factory = nullptr
    ) {
        auto& tbl_cols = setup.table->columns();
        yugawara::storage::index const& idx =
            use_secondary ? *setup.secondary_idx : *setup.primary_idx;

        std::vector<relation::join_find::column> cols;
        std::vector<descriptor::variable> dests;
        for (std::size_t i = 0; i < tbl_cols.size(); ++i) {
            auto dest = bindings_.stream_variable("c" + std::to_string(i));
            cols.emplace_back(bindings_(tbl_cols[i]), dest);
            dests.push_back(dest);
        }

        auto make_keys = [&](scan_endpoint_spec const& spec) {
            std::vector<relation::join_scan::key> keys;
            for (std::size_t i = 0; i < spec.col_indices.size(); ++i) {
                keys.emplace_back(
                    bindings_(tbl_cols[spec.col_indices[i]]),
                    scalar::variable_reference{spec.source_vars[i]}
                );
            }
            return keys;
        };

        std::unique_ptr<scalar::expression> condition;
        if (condition_factory) {
            condition = condition_factory(dests);
        }

        auto& target = emplace_operator<relation::join_scan>(
            relation::join_kind::inner,
            bindings_(idx),
            std::move(cols),
            relation::join_scan::endpoint{make_keys(lower), lower.kind},
            relation::join_scan::endpoint{make_keys(upper), upper.kind},
            std::move(condition)
        );

        for (std::size_t i = 0; i < target.columns().size(); ++i) {
            yugawara::analyzer::variable_resolution r{
                takatori::util::clone_shared(tbl_cols[i].type())};
            variable_map_->bind(target.columns()[i].source(), r, true);
            variable_map_->bind(target.columns()[i].destination(), r, true);
        }
        for (std::size_t i = 0; i < lower.col_indices.size(); ++i) {
            expression_map_->bind(
                target.lower().keys()[i].value(),
                yugawara::analyzer::expression_resolution{
                    takatori::util::clone_shared(tbl_cols[lower.col_indices[i]].type())});
        }
        for (std::size_t i = 0; i < upper.col_indices.size(); ++i) {
            expression_map_->bind(
                target.upper().keys()[i].value(),
                yugawara::analyzer::expression_resolution{
                    takatori::util::clone_shared(tbl_cols[upper.col_indices[i]].type())});
        }
        if (target.condition()) {
            bind_int4_compare_condition(*target.condition());
        }
        return target;
    }

    /**
     * @brief Bind a compare condition expression assuming int4 operands.
     * @param condition  the condition expression; must be a scalar::compare node
     */
    void bind_int4_compare_condition(scalar::expression& condition) {
        expression_map_->bind(condition, t::boolean{});
        auto& cmp = static_cast<scalar::compare&>(condition);
        expression_map_->bind(cmp.left(), t::int4{});
        expression_map_->bind(cmp.right(), t::int4{});
    }

    /**
     * @brief Wire the process graph, build processor_info, construct the join_scan
     *     operator, and return a join_scan_executor.
     *
     * @details Uses C++17 guaranteed copy elision: the returned prvalue is
     *     constructed directly in the caller's variable so ctx_ references into
     *     variables_ and task_ctx_ remain valid.  Graph wiring happens before
     *     create_processor_info() to ensure all stream variables appear in the
     *     single processor_info block.
     *
     * @param up            upstream take_flat node
     * @param target        the join_scan relation node
     * @param primary_idx   primary index
     * @param secondary_idx secondary index pointer, or nullptr
     * @param down          downstream verifier sink (take() is called here)
     * @param primary_stg   KVS storage for the primary index
     * @param secondary_stg KVS storage for the secondary index, or nullptr
     * @param tx            active transaction (shared ownership)
     * @param host_vars     optional host variable table for prepared-statement parameters
     * @param jk            join kind; defaults to inner
     * @return newly constructed join_scan_executor
     */
    join_scan_executor make_join_scan_executor(
        relation::step::take_flat& up,
        relation::join_scan& target,
        yugawara::storage::index const& primary_idx,
        yugawara::storage::index const* secondary_idx,
        record_verifier_sink& down,
        std::unique_ptr<kvs::storage> primary_stg,
        std::unique_ptr<kvs::storage> secondary_stg,
        std::shared_ptr<transaction_context> tx,
        variable_table* host_vars = nullptr,
        relation::join_kind jk = relation::join_kind::inner
    ) {
        up.output() >> target.left();
        target.output() >> down.input();
        create_processor_info(host_vars);
        request_context_.transaction(tx);
        return join_scan_executor{
            *processor_info_,
            jk,
            primary_idx,
            target.columns(),
            target.lower().keys(),
            utils::from(target.lower().kind()),
            target.upper().keys(),
            utils::from(target.upper().kind()),
            target.condition(),
            secondary_idx,
            down.take(),
            std::move(primary_stg),
            std::move(secondary_stg),
            tx.get(),
            tx,
            &resource_,
            &varlen_resource_,
            &request_context_
        };
    }
};

} // namespace jogasaki::executor::process::impl::ops
