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
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/descriptor/variable.h>
#include <takatori/relation/find.h>
#include <takatori/relation/sort_direction.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/variable_reference.h>
#include <takatori/tree/tree_fragment_vector.h>
#include <takatori/type/primitive.h>
#include <takatori/util/clonable.h>
#include <takatori/util/sequence_view.h>
#include <takatori/value/primitive.h>
#include <yugawara/analyzer/expression_resolution.h>
#include <yugawara/analyzer/variable_resolution.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/executor/process/impl/ops/find.h>
#include <jogasaki/executor/process/impl/ops/find_context.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variables_view.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs_test_base.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/operator_test_utils.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/field_types.h>

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

using yugawara::variable::nullity;
using yugawara::storage::index;

/**
 * @brief Bundle of runtime objects needed to invoke and inspect the find operator.
 * @details find is an active operator: it reads from KVS and populates variables
 *     via the downstream verifier.  Always create via find_test::make_find_executor().
 *     The struct must not be copy- or move-constructed.
 */
struct find_executor {
    find op_;
    variable_table_list variables_list_;
    mock::task_context task_ctx_;
    class find_context ctx_;

    find_executor(
        processor_info const& info,
        takatori::tree::tree_fragment_vector<find::key> const& keys,
        yugawara::storage::index const& primary_idx,
        sequence_view<find::column const> columns,
        yugawara::storage::index const* secondary_idx,
        std::unique_ptr<operator_base> downstream,
        std::unique_ptr<kvs::storage> primary_stg,
        std::unique_ptr<kvs::storage> secondary_stg,
        transaction_context* tx,
        memory::lifo_paged_memory_resource* res,
        memory::lifo_paged_memory_resource* varlen_res,
        request_context* req_ctx
    ) :
        op_{0, info, 0, keys, primary_idx, columns, secondary_idx, std::move(downstream)},
        variables_list_{},
        task_ctx_{{}, {}, {}, {}},
        ctx_{&task_ctx_, variables_view{variables_list_, 0},
            std::move(primary_stg), std::move(secondary_stg), tx, res, varlen_res, nullptr}
    {
        variables_list_.emplace_back(info.vars_info_list()[0]);
        ctx_.task_context().work_context(std::make_unique<impl::work_context>(
            req_ctx, 0, op_.block_index(), nullptr, nullptr, nullptr, nullptr, false, false
        ));
    }
};

class find_test :
    public test_root,
    public kvs_test_base,
    public operator_test_utils {

public:
    void SetUp() override {
        kvs_db_setup();
    }

    void TearDown() override {
        kvs_db_teardown();
    }

    void run_secondary_index(bool nullable, relation::sort_direction dir);
    void run_composite_secondary_key(
        bool first_col_nullable,
        relation::sort_direction dir0,
        relation::sort_direction dir1
    );

    /**
     * @brief Insert a find relation node into the process graph.
     *
     * @details Wires all table columns as output columns.  Column types and key
     *     expression types are derived from the table metadata, so no explicit
     *     type parameters are needed.
     *
     * @param setup            table and index configuration for this test case
     * @param key_col_indices  0-based column indices of the table used as search keys
     * @param key_exprs        search key value expressions, one per key column
     *                         (ownership transferred)
     * @param use_secondary    if true, the find node targets setup.secondary_idx;
     *                         otherwise setup.primary_idx
     * @return reference to the newly inserted find node
     */
    relation::find& add_find_node(
        table_setup const& setup,
        std::vector<std::size_t> const& key_col_indices,
        std::vector<std::unique_ptr<scalar::expression>> key_exprs,
        bool use_secondary = false
    ) {
        auto& tbl_cols = setup.table->columns();
        yugawara::storage::index const& idx =
            use_secondary ? *setup.secondary_idx : *setup.primary_idx;
        std::vector<relation::find::column> cols;
        for (std::size_t i = 0; i < tbl_cols.size(); ++i) {
            cols.emplace_back(
                bindings_(tbl_cols[i]),
                bindings_.stream_variable("c" + std::to_string(i))
            );
        }
        std::vector<relation::find::key> keys{};
        for (std::size_t i = 0; i < key_col_indices.size(); ++i) {
            keys.emplace_back(bindings_(tbl_cols[key_col_indices[i]]), std::move(key_exprs[i]));
        }
        auto& target = process_.operators().insert(relation::find{
            bindings_(idx), std::move(cols), std::move(keys)
        });
        // Bind column types derived from the table's column definitions.
        for (std::size_t i = 0; i < target.columns().size(); ++i) {
            yugawara::analyzer::variable_resolution r{
                takatori::util::clone_shared(tbl_cols[i].type())};
            variable_map_->bind(target.columns()[i].source(), r, true);
            variable_map_->bind(target.columns()[i].destination(), r, true);
        }
        // Bind key expression types derived from the table's column definitions.
        for (std::size_t i = 0; i < key_col_indices.size(); ++i) {
            expression_map_->bind(
                target.keys()[i].value(),
                yugawara::analyzer::expression_resolution{
                    takatori::util::clone_shared(tbl_cols[key_col_indices[i]].type())});
        }
        return target;
    }

    /**
     * @brief Construct a vector of key expressions from variadic unique_ptr arguments.
     * @details Convenience helper for add_find_node call sites.
     */
    template<typename... Args>
    static std::vector<std::unique_ptr<scalar::expression>> make_exprs(Args&&... args) {
        std::vector<std::unique_ptr<scalar::expression>> v;
        (v.push_back(std::forward<Args>(args)), ...);
        return v;
    }

    /**
     * @brief Wire the process graph, build processor_info, construct the find operator,
     *     and return a find_executor.
     *
     * @param target         the find relation node
     * @param primary_idx    the primary index to use
     * @param secondary_idx  secondary index, or nullptr if not used
     * @param down           downstream verifier sink (take() is called here)
     * @param primary_stg    KVS storage handle for the primary index
     * @param secondary_stg  KVS storage handle for the secondary index, or nullptr
     * @param tx             the active transaction context
     * @param host_vars      optional host variable table
     * @return newly constructed find_executor
     */
    find_executor make_find_executor(
        relation::find& target,
        yugawara::storage::index const& primary_idx,
        yugawara::storage::index const* secondary_idx,
        record_verifier_sink& down,
        std::unique_ptr<kvs::storage> primary_stg,
        std::unique_ptr<kvs::storage> secondary_stg,
        transaction_context* tx,
        variable_table* host_vars = nullptr
    ) {
        target.output() >> down.input();
        create_processor_info(host_vars);
        return find_executor{
            *processor_info_,
            target.keys(),
            primary_idx,
            target.columns(),
            secondary_idx,
            down.take(),
            std::move(primary_stg),
            std::move(secondary_stg),
            tx,
            &resource_,
            &varlen_resource_,
            &request_context_
        };
    }
};

TEST_F(find_test, simple) {
    auto setup = prepare_indices(
        {"T0", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );
    auto& target = add_find_node(
        setup, {0}, make_exprs(
            std::make_unique<scalar::immediate>(takatori::value::int4(20), takatori::type::int4())
        )
    );
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 200), *db_);
    auto tx = wrap(db_->create_transaction());
    auto ex = make_find_executor(
        target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx.get()
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 200)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(find_test, multiple_types) {
    auto setup = prepare_indices(
        {"T0", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::float8(), nullity{false}},
            {"C2", t::int8(), nullity{false}},
        }},
        {0}, {}
    );
    auto& target = add_find_node(
        setup, {0}, make_exprs(
            std::make_unique<scalar::immediate>(takatori::value::int4(20), takatori::type::int4())
        )
    );
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    put_row(setup, create_nullable_record<kind::int4, kind::float8, kind::int8>(10, 1.0, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200), *db_);
    auto tx = wrap(db_->create_transaction());
    auto ex = make_find_executor(
        target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx.get()
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

void find_test::run_secondary_index(bool nullable, relation::sort_direction dir) {
    nullity c1_nullity{nullable};
    auto setup = prepare_indices(
        {"T0", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), c1_nullity},
        }},
        {0}, {1}, {dir}
    );
    auto& target = add_find_node(
        setup, {1}, make_exprs(
            std::make_unique<scalar::immediate>(takatori::value::int4(200), takatori::type::int4())
        ), true
    );
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 200), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(21, 200), *db_);
    if (nullable) {
        // NULL in the secondary key column must not match the equality find
        put_row(setup, create_nullable_record<kind::int4, kind::int4>(99, std::nullopt), *db_);
    }
    auto tx = wrap(db_->create_transaction());
    auto ex = make_find_executor(
        target, *setup.primary_idx, setup.secondary_idx.get(), down,
        get_storage(*db_, setup.primary_idx->simple_name()),
        get_storage(*db_, setup.secondary_idx->simple_name()),
        tx.get()
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 200)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(21, 200)), result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(find_test, secondary_index) {
    run_secondary_index(false, relation::sort_direction::ascendant);
}

TEST_F(find_test, secondary_index_nullable) {
    run_secondary_index(true, relation::sort_direction::ascendant);
}

TEST_F(find_test, secondary_index_desc) {
    run_secondary_index(false, relation::sort_direction::descendant);
}

TEST_F(find_test, secondary_index_nullable_desc) {
    run_secondary_index(true, relation::sort_direction::descendant);
}

TEST_F(find_test, composite_primary_key) {
    auto setup = prepare_indices(
        {"T0", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
            {"C2", t::int4(), nullity{false}},
        }},
        {0, 1}, {}
    );
    auto& target = add_find_node(
        setup, {0, 1}, make_exprs(
            std::make_unique<scalar::immediate>(takatori::value::int4(20), takatori::type::int4()),
            std::make_unique<scalar::immediate>(takatori::value::int4(2), takatori::type::int4())
        )
    );
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 1, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(20, 2, 200), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(20, 3, 300), *db_);
    auto tx = wrap(db_->create_transaction());
    auto ex = make_find_executor(
        target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx.get()
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(20, 2, 200)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

void find_test::run_composite_secondary_key(
    bool first_col_nullable,
    relation::sort_direction dir0,
    relation::sort_direction dir1
) {
    nullity c1_nullity{first_col_nullable};
    nullity c2_nullity{! first_col_nullable};
    auto setup = prepare_indices(
        {"T0", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), c1_nullity},
            {"C2", t::int4(), c2_nullity},
        }},
        {0}, {1, 2}, {dir0, dir1}
    );
    auto& target = add_find_node(
        setup, {1, 2}, make_exprs(
            std::make_unique<scalar::immediate>(takatori::value::int4(2), takatori::type::int4()),
            std::make_unique<scalar::immediate>(takatori::value::int4(2), takatori::type::int4())
        ), true
    );
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 1, 1), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(20, 2, 2), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(21, 2, 2), *db_);
    if (first_col_nullable) {
        // C1 is nullable: row with C1=NULL must not match (C1=2, C2=2)
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(99, std::nullopt, 2), *db_);
    } else {
        // C2 is nullable: row with C2=NULL must not match (C1=2, C2=2)
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(99, 2, std::nullopt), *db_);
    }
    auto tx = wrap(db_->create_transaction());
    auto ex = make_find_executor(
        target, *setup.primary_idx, setup.secondary_idx.get(), down,
        get_storage(*db_, setup.primary_idx->simple_name()),
        get_storage(*db_, setup.secondary_idx->simple_name()),
        tx.get()
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(20, 2, 2)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(21, 2, 2)), result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(find_test, composite_key_secondary_c1nullable_asc_asc) {
    run_composite_secondary_key(true, relation::sort_direction::ascendant, relation::sort_direction::ascendant);
}

TEST_F(find_test, composite_key_secondary_c1nullable_asc_desc) {
    run_composite_secondary_key(true, relation::sort_direction::ascendant, relation::sort_direction::descendant);
}

TEST_F(find_test, composite_key_secondary_c1nullable_desc_asc) {
    run_composite_secondary_key(true, relation::sort_direction::descendant, relation::sort_direction::ascendant);
}

TEST_F(find_test, composite_key_secondary_c1nullable_desc_desc) {
    run_composite_secondary_key(true, relation::sort_direction::descendant, relation::sort_direction::descendant);
}

TEST_F(find_test, composite_key_secondary_c2nullable_asc_asc) {
    run_composite_secondary_key(false, relation::sort_direction::ascendant, relation::sort_direction::ascendant);
}

TEST_F(find_test, composite_key_secondary_c2nullable_asc_desc) {
    run_composite_secondary_key(false, relation::sort_direction::ascendant, relation::sort_direction::descendant);
}

TEST_F(find_test, composite_key_secondary_c2nullable_desc_asc) {
    run_composite_secondary_key(false, relation::sort_direction::descendant, relation::sort_direction::ascendant);
}

TEST_F(find_test, composite_key_secondary_c2nullable_desc_desc) {
    run_composite_secondary_key(false, relation::sort_direction::descendant, relation::sort_direction::descendant);
}


TEST_F(find_test, host_variable) {
    auto setup = prepare_indices(
        {"T0", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::float8(), nullity{false}},
            {"C2", t::int8(), nullity{false}},
        }},
        {0}, {}
    );
    auto host_variable_record = create_nullable_record<kind::int4>(20);
    auto p0 = bindings_(register_variable("p0", kind::int4));
    variable_table_info host_variable_info{
        std::unordered_map<descriptor::variable, std::size_t>{{p0, 0}},
        std::unordered_map<std::string, descriptor::variable>{{"p0", p0}},
        host_variable_record.record_meta()
    };
    variable_table host_variables{host_variable_info};
    host_variables.store().set(host_variable_record.ref());

    auto& target = add_find_node(
        setup, {0}, make_exprs(
            std::make_unique<scalar::variable_reference>(p0)
        )
    );
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    put_row(setup, create_nullable_record<kind::int4, kind::float8, kind::int8>(10, 1.0, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200), *db_);
    auto tx = wrap(db_->create_transaction());
    auto ex = make_find_executor(
        target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx.get(),
        &host_variables
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });

    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

}

