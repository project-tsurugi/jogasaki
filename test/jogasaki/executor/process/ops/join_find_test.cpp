/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/descriptor/variable.h>
#include <takatori/relation/join_find.h>
#include <takatori/relation/join_kind.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/comparison_operator.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/variable_reference.h>
#include <takatori/type/primitive.h>
#include <takatori/value/primitive.h>
#include <takatori/util/clonable.h>
#include <takatori/util/optional_ptr.h>
#include <yugawara/analyzer/expression_resolution.h>
#include <yugawara/analyzer/variable_resolution.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/executor/process/impl/ops/index_join.h>
#include <jogasaki/executor/process/impl/ops/index_join_context.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs_test_base.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/operator_test_utils.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/transaction_context.h>

#include "verifier.h"

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace jogasaki::mock;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;

using kind = field_type_kind;
using yugawara::variable::nullity;
using scalar::compare;
using scalar::comparison_operator;
using varref = scalar::variable_reference;

/**
 * @brief Bundle of runtime objects needed to invoke the join_find operator.
 * @details join_find is a passive record operator: upstream records supply
 *     KVS key values; the operator looks them up and writes results back into
 *     the single combined variable block shared by all stream variables.
 *     match_info_ is a member (not a local variable) so that the join_find_matcher
 *     stored in ctx_ holds a valid reference throughout the executor's lifetime.
 *     ctx_ is emplaced in the constructor body after op_ and match_info_ are
 *     fully initialised.  The struct must not be copy- or move-constructed.
 *     Always create via join_find_test::make_join_find_executor().
 */
struct join_find_executor {
    join_find op_;
    details::match_info_find match_info_;
    variable_table_list variables_list_;
    mock::task_context task_ctx_;
    std::optional<join_find_context> ctx_;

    join_find_executor(
        processor_info const& pinfo,
        relation::join_kind kind,
        yugawara::storage::index const& primary_idx,
        takatori::util::sequence_view<relation::join_find::column const> columns,
        takatori::tree::tree_fragment_vector<relation::join_find::key> const& keys,
        takatori::util::optional_ptr<scalar::expression const> condition,
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
        op_{kind, 0, pinfo, 0, primary_idx, columns, keys,
            condition, secondary_idx, std::move(downstream)},
        match_info_{op_.match_info().key_fields_,
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
            std::make_unique<join_find_matcher>(
                secondary_idx != nullptr, match_info_, op_.key_columns(), op_.value_columns()),
            res,
            varlen_res,
            nullptr
        );
        ctx_->task_context().work_context(std::make_unique<impl::work_context>(
            req_ctx, 0, op_.block_index(), nullptr, nullptr, nullptr, tx_shared, false, false
        ));
    }

    join_find_executor(join_find_executor const&) = delete;
    join_find_executor& operator=(join_find_executor const&) = delete;
    join_find_executor(join_find_executor&&) = delete;
    join_find_executor& operator=(join_find_executor&&) = delete;
};

class join_find_test :
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
     * @brief Insert a join_find relation node into the process graph.
     *
     * @details All table columns are mapped to fresh stream variables as output
     *     columns.  Column and key-expression types are derived from the table
     *     metadata.  If condition_factory is provided it is called with the
     *     column destination variables so the condition can reference them; the
     *     returned expression is bound assuming a top-level int4 compare.
     *
     * @param setup              table and index configuration
     * @param key_col_indices    0-based table column indices used as join keys
     * @param key_source_vars    upstream stream variables supplying key values,
     *                           one per entry in key_col_indices
     * @param use_secondary      if true, targets setup.secondary_idx; otherwise
     *                           setup.primary_idx
     * @param condition_factory  optional factory called with column destinations
     *                           to produce a condition expression (int4 compare)
     * @return reference to the newly inserted node
     */
    using condition_factory_t =
        std::function<std::unique_ptr<scalar::expression>(std::vector<descriptor::variable> const&)>;

    relation::join_find& add_join_find_node(
        table_setup const& setup,
        std::vector<std::size_t> const& key_col_indices,
        std::vector<descriptor::variable> const& key_source_vars,
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
        std::vector<relation::join_find::key> keys;
        for (std::size_t i = 0; i < key_col_indices.size(); ++i) {
            keys.emplace_back(
                bindings_(tbl_cols[key_col_indices[i]]),
                varref{key_source_vars[i]}
            );
        }
        std::unique_ptr<scalar::expression> condition;
        if (condition_factory) {
            condition = condition_factory(dests);
        }
        auto& target = emplace_operator<relation::join_find>(
            relation::join_kind::inner,
            bindings_(idx),
            std::move(cols),
            std::move(keys),
            std::move(condition)
        );
        for (std::size_t i = 0; i < target.columns().size(); ++i) {
            yugawara::analyzer::variable_resolution r{
                takatori::util::clone_shared(tbl_cols[i].type())};
            variable_map_->bind(target.columns()[i].source(), r, true);
            variable_map_->bind(target.columns()[i].destination(), r, true);
        }
        for (std::size_t i = 0; i < key_col_indices.size(); ++i) {
            expression_map_->bind(
                target.keys()[i].value(),
                yugawara::analyzer::expression_resolution{
                    takatori::util::clone_shared(tbl_cols[key_col_indices[i]].type())});
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
     * @brief Wire the process graph, build processor_info, construct the join_find
     *     operator, and return a join_find_executor.
     *
     * @details Uses C++17 guaranteed copy elision: the returned prvalue is
     *     constructed directly in the caller's variable so ctx_ references into
     *     variables_ and task_ctx_ remain valid.  Graph wiring happens before
     *     create_processor_info() to ensure all stream variables appear in the
     *     single processor_info block.
     *
     * @param up           upstream take_flat node
     * @param target       the join_find relation node
     * @param primary_idx  primary index
     * @param secondary_idx secondary index pointer, or nullptr
     * @param down         downstream verifier sink (take() is called here)
     * @param primary_stg  KVS storage for the primary index
     * @param secondary_stg KVS storage for the secondary index, or nullptr
     * @param tx           active transaction (shared ownership)
     * @param host_vars    optional host variable table for prepared-statement
     *                     parameters used in condition expressions
     * @param jk           join kind; defaults to inner
     * @return newly constructed join_find_executor
     */
    join_find_executor make_join_find_executor(
        relation::step::take_flat& up,
        relation::join_find& target,
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
        return join_find_executor{
            *processor_info_,
            jk,
            primary_idx,
            target.columns(),
            target.keys(),
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

    /**
     * @brief Run a composite secondary index join_find test.
     *
     * @details Table has three int4 columns: C0 (primary key, non-nullable),
     *     C1 and C2 forming the composite secondary key with the given directions.
     *     When first_col_nullable is true, C1 is nullable and C2 is non-nullable;
     *     otherwise C1 is non-nullable and C2 is nullable.  A null row is inserted
     *     for the nullable column to confirm it does not appear in the results.
     *
     * @param first_col_nullable  when true C1 is nullable and C2 is non-nullable;
     *                            when false C1 is non-nullable and C2 is nullable
     * @param first_dir           sort direction for C1 in the secondary index
     * @param second_dir          sort direction for C2 in the secondary index
     */
    void do_composite_secondary_key_test(
        bool first_col_nullable,
        relation::sort_direction first_dir,
        relation::sort_direction second_dir
    ) {
        nullity c1_nullity{first_col_nullable};
        nullity c2_nullity{! first_col_nullable};
        auto setup = prepare_indices(
            {"T1", {
                {"C0", t::int4(), nullity{false}},
                {"C1", t::int4(), c1_nullity},
                {"C2", t::int4(), c2_nullity},
            }},
            {0}, {1, 2}, {first_dir, second_dir}
        );
        auto input = create_nullable_record<kind::int4, kind::int4>(1, 2);
        auto [up, in] = add_upstream_record_provider(input.record_meta());
        auto& target = add_join_find_node(setup, {1, 2}, {in[0], in[1]}, true);
        auto verifier_vars = in.vars_;
        auto out_vars = destinations(target.columns());
        verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
        auto down = add_downstream_record_verifier(std::move(verifier_vars));

        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 1, 2), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(20, 1, 2), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(30, 1, 3), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(40, 2, 2), *db_);
        if (first_col_nullable) {
            put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(
                std::optional<int32_t>{50}, std::optional<int32_t>{}, std::optional<int32_t>{2}
            ), *db_);
        } else {
            put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(
                std::optional<int32_t>{60}, std::optional<int32_t>{1}, std::optional<int32_t>{}
            ), *db_);
        }

        auto tx = wrap(db_->create_transaction());
        auto ex = make_join_find_executor(
            up, target, *setup.primary_idx, setup.secondary_idx.get(), down,
            get_storage(*db_, setup.primary_idx->simple_name()),
            get_storage(*db_, setup.secondary_idx->simple_name()),
            tx
        );

        std::vector<basic_record> result{};
        down.set_body([&]() {
            result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
        });

        set_variables(ex.variables_list_[0], in, input.ref());
        ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
        ex.ctx_->release();
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 1, 2)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(20, 1, 2)), result[1]);
        ASSERT_EQ(status::ok, tx->commit());
    }
};

TEST_F(join_find_test, simple) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );
    auto input = create_nullable_record<kind::int4, kind::int4>(1, 10);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    auto& target = add_join_find_node(setup, {0}, {in[0]});
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(1, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(2, 200), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(3, 300), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    set_variables(ex.variables_list_[0], in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 100)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, secondary_index) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {1}
    );
    auto input = create_nullable_record<kind::int4, kind::int4>(2, 20);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    auto& target = add_join_find_node(setup, {1}, {in[1]}, true);
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(100, 10), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(200, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(201, 20), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, setup.secondary_idx.get(), down,
        get_storage(*db_, setup.primary_idx->simple_name()),
        get_storage(*db_, setup.secondary_idx->simple_name()),
        tx
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    set_variables(ex.variables_list_[0], in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(200, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(201, 20)), result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, host_variable_with_condition_expr) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );

    auto host_variable_record = create_nullable_record<kind::int4>(10);
    auto p0 = bindings_(register_variable("p0", kind::int4));
    variable_table_info host_variable_info{
        std::unordered_map<descriptor::variable, std::size_t>{{p0, 0}},
        std::unordered_map<std::string, descriptor::variable>{{"p0", p0}},
        host_variable_record.record_meta()
    };
    variable_table host_variables{host_variable_info};
    host_variables.store().set(host_variable_record.ref());

    auto input = create_nullable_record<kind::int4, kind::int4>(1, 10);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    auto& target = add_join_find_node(setup, {0}, {in[0]}, false,
        [&](auto const&) {
            return std::make_unique<compare>(
                comparison_operator::equal, varref{in[1]}, varref{p0});
        });
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(1, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(2, 200), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(3, 300), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx,
        &host_variables
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    set_variables(ex.variables_list_[0], in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 100)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, multiple_types) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::float8(), nullity{false}},
            {"C2", t::int8(), nullity{false}},
        }},
        {0}, {}
    );
    auto input = create_nullable_record<kind::int4>(20);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    auto& target = add_join_find_node(setup, {0}, {in[0]});
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::float8, kind::int8>(10, 1.0, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::float8, kind::int8>(30, 3.0, 300), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    set_variables(ex.variables_list_[0], in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, semi_join_primary) {
    // Semi join: emit upstream row exactly once when key matches; no emit when no match.
    // Also verifies that right-side table columns are nullified when downstream is called.
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );
    auto input_match    = create_nullable_record<kind::int4>(1);
    auto input_no_match = create_nullable_record<kind::int4>(5);
    auto [up, in] = add_upstream_record_provider(input_match.record_meta());
    auto& target = add_join_find_node(setup, {0}, {in[0]});
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(1, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(2, 200), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx,
        nullptr, relation::join_kind::semi
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        // right-side vars must be null (semi join must not project them)
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(std::nullopt, std::nullopt)),
            get_variables(ex.variables_list_[0], out_vars));
        result.emplace_back(get_variables(ex.variables_list_[0], in.vars_));
    });

    // C0=1 exists → semi emits upstream row exactly once
    set_variables(ex.variables_list_[0], in, input_match.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);

    // C0=5 doesn't exist → semi does not emit
    set_variables(ex.variables_list_[0], in, input_no_match.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ASSERT_EQ(1, result.size()); // unchanged

    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, anti_join_primary) {
    // Anti join: emit upstream row when key has no match; no emit when match exists.
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );
    auto input_match    = create_nullable_record<kind::int4>(1);
    auto input_no_match = create_nullable_record<kind::int4>(5);
    auto [up, in] = add_upstream_record_provider(input_match.record_meta());
    auto& target = add_join_find_node(setup, {0}, {in[0]});
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(1, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(2, 200), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx,
        nullptr, relation::join_kind::anti
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], in.vars_));
    });

    // C0=1 exists → anti does not emit
    set_variables(ex.variables_list_[0], in, input_match.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ASSERT_EQ(0, result.size());

    // C0=5 doesn't exist → anti emits upstream row once
    set_variables(ex.variables_list_[0], in, input_no_match.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(5)), result[0]);

    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, semi_join_secondary) {
    // Semi join via secondary index: multiple right-side matches → emit upstream row exactly once.
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {1}
    );
    auto input_match    = create_nullable_record<kind::int4, kind::int4>(0, 2);
    auto input_no_match = create_nullable_record<kind::int4, kind::int4>(0, 5);
    auto [up, in] = add_upstream_record_provider(input_match.record_meta());
    auto& target = add_join_find_node(setup, {1}, {in[1]}, true);
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(100, 1), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(200, 2), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(201, 2), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, setup.secondary_idx.get(), down,
        get_storage(*db_, setup.primary_idx->simple_name()),
        get_storage(*db_, setup.secondary_idx->simple_name()),
        tx, nullptr, relation::join_kind::semi
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], in.vars_));
    });

    // C1=2 has two right-side matches → semi emits exactly once
    set_variables(ex.variables_list_[0], in, input_match.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(0, 2)), result[0]);

    // C1=5 has no match → semi does not emit
    set_variables(ex.variables_list_[0], in, input_no_match.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ASSERT_EQ(1, result.size()); // unchanged

    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, anti_join_secondary) {
    // Anti join via secondary index: no emit when match exists; emit once when no match.
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {1}
    );
    auto input_match    = create_nullable_record<kind::int4, kind::int4>(0, 2);
    auto input_no_match = create_nullable_record<kind::int4, kind::int4>(0, 5);
    auto [up, in] = add_upstream_record_provider(input_match.record_meta());
    auto& target = add_join_find_node(setup, {1}, {in[1]}, true);
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(100, 1), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(200, 2), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(201, 2), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, setup.secondary_idx.get(), down,
        get_storage(*db_, setup.primary_idx->simple_name()),
        get_storage(*db_, setup.secondary_idx->simple_name()),
        tx, nullptr, relation::join_kind::anti
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], in.vars_));
    });

    // C1=2 has right-side matches → anti does not emit
    set_variables(ex.variables_list_[0], in, input_match.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ASSERT_EQ(0, result.size());

    // C1=5 has no match → anti emits once
    set_variables(ex.variables_list_[0], in, input_no_match.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(0, 5)), result[0]);

    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, semi_join_primary_condition_filtered) {
    // Key matches but condition is false → semi does not emit (no condition-satisfying match).
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );
    auto input = create_nullable_record<kind::int4>(1);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    // condition: C1 == 999; always false because the row has C1=100
    auto& target = add_join_find_node(setup, {0}, {in[0]}, false,
        [](auto const& dests) {
            return std::make_unique<compare>(
                comparison_operator::equal,
                varref{dests[1]},
                scalar::immediate{takatori::value::int4{999}, takatori::type::int4{}}
            );
        });
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(1, 100), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx,
        nullptr, relation::join_kind::semi
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], in.vars_));
    });

    set_variables(ex.variables_list_[0], in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    // key matches (C0=1) but C1=100 ≠ 999 → condition false → semi must not emit
    ASSERT_EQ(0, result.size());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, anti_join_primary_condition_filtered) {
    // Key matches but condition is false → anti emits (no condition-satisfying match found).
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );
    auto input = create_nullable_record<kind::int4>(1);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    // condition: C1 == 999; always false because the row has C1=100
    auto& target = add_join_find_node(setup, {0}, {in[0]}, false,
        [](auto const& dests) {
            return std::make_unique<compare>(
                comparison_operator::equal,
                varref{dests[1]},
                scalar::immediate{takatori::value::int4{999}, takatori::type::int4{}}
            );
        });
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(1, 100), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx,
        nullptr, relation::join_kind::anti
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], in.vars_));
    });

    set_variables(ex.variables_list_[0], in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    // key matches (C0=1) but C1=100 ≠ 999 → condition false → anti emits (no satisfying match)
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, left_outer) {
    // inner join emits only matched rows; left_outer also emits a null-padded
    // row for inputs that have no match in the table.
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );
    auto input_match    = create_nullable_record<kind::int4>(10);
    auto input_no_match = create_nullable_record<kind::int4>(30);
    auto [up, in] = add_upstream_record_provider(input_match.record_meta());
    auto& target = add_join_find_node(setup, {0}, {in[0]});
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 200), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx,
        nullptr, relation::join_kind::left_outer
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    set_variables(ex.variables_list_[0], in, input_match.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 100)), result[0]);

    set_variables(ex.variables_list_[0], in, input_no_match.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(std::nullopt, std::nullopt)), result[1]);

    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, left_outer_condition_filtered) {
    // Row is found by key but the condition is false; left_outer emits null-padded output.
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );

    auto input = create_nullable_record<kind::int4>(1);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    // condition: C1 == 999 is false for the inserted row (C1=100)
    auto& target = add_join_find_node(setup, {0}, {in[0]}, false,
        [](auto const& dests) {
            return std::make_unique<compare>(
                comparison_operator::equal,
                varref{dests[1]},
                scalar::immediate{takatori::value::int4{999}, takatori::type::int4{}}
            );
        });
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(1, 100), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx,
        nullptr, relation::join_kind::left_outer
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    set_variables(ex.variables_list_[0], in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(std::nullopt, std::nullopt)),
        result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, composite_key_primary) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
            {"C2", t::int4(), nullity{false}},
        }},
        {0, 1}, {}
    );
    auto input = create_nullable_record<kind::int4, kind::int4>(1, 10);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    auto& target = add_join_find_node(setup, {0, 1}, {in[0], in[1]});
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 20, 200), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 300), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    set_variables(ex.variables_list_[0], in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 100)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, secondary_index_desc) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {1}, {desc}
    );
    auto input = create_nullable_record<kind::int4, kind::int4>(2, 20);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    auto& target = add_join_find_node(setup, {1}, {in[1]}, true);
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(100, 10), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(200, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(201, 20), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_find_executor(
        up, target, *setup.primary_idx, setup.secondary_idx.get(), down,
        get_storage(*db_, setup.primary_idx->simple_name()),
        get_storage(*db_, setup.secondary_idx->simple_name()),
        tx
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    set_variables(ex.variables_list_[0], in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(200, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(201, 20)), result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_find_test, composite_key_secondary_nullable_nonnullable_asc_asc) {
    do_composite_secondary_key_test(true, asc, asc);
}

TEST_F(join_find_test, composite_key_secondary_nullable_nonnullable_asc_desc) {
    do_composite_secondary_key_test(true, asc, desc);
}

TEST_F(join_find_test, composite_key_secondary_nullable_nonnullable_desc_asc) {
    do_composite_secondary_key_test(true, desc, asc);
}

TEST_F(join_find_test, composite_key_secondary_nullable_nonnullable_desc_desc) {
    do_composite_secondary_key_test(true, desc, desc);
}

TEST_F(join_find_test, composite_key_secondary_nonnullable_nullable_asc_asc) {
    do_composite_secondary_key_test(false, asc, asc);
}

TEST_F(join_find_test, composite_key_secondary_nonnullable_nullable_asc_desc) {
    do_composite_secondary_key_test(false, asc, desc);
}

TEST_F(join_find_test, composite_key_secondary_nonnullable_nullable_desc_asc) {
    do_composite_secondary_key_test(false, desc, asc);
}

TEST_F(join_find_test, composite_key_secondary_nonnullable_nullable_desc_desc) {
    do_composite_secondary_key_test(false, desc, desc);
}

} // namespace jogasaki::executor::process::impl::ops
