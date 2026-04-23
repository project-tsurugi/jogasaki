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
#include <algorithm>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/scalar/compare.h>
#include <takatori/scalar/comparison_operator.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/variable_reference.h>
#include <takatori/value/primitive.h>

#include <jogasaki/executor/process/ops/join_scan_test_common.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace jogasaki::mock;

using yugawara::variable::nullity;
using scalar::compare;
using scalar::comparison_operator;
using varref = scalar::variable_reference;
using kind = field_type_kind;

class join_scan_test : public join_scan_test_base {};

TEST_F(join_scan_test, simple) {
    // PK=(C0,C1), range C0 in [1,2] via prefixed_inclusive on both bounds.
    // Rows (1,100), (1,101) and (2,200) are within the range; (3,300) is not.
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0, 1}, {}
    );
    auto input = create_nullable_record<kind::int4, kind::int4>(1, 2);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    scan_endpoint_spec lower{{0}, {in[0]}, relation::endpoint_kind::prefixed_inclusive};
    scan_endpoint_spec upper{{0}, {in[1]}, relation::endpoint_kind::prefixed_inclusive};
    auto& target = add_join_scan_node(setup, lower, upper);
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(1, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(1, 101), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(2, 200), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(3, 300), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_, destinations(target.columns())));
    });

    set_variables(ex.variables_, in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(3, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 101)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2, 200)), result[2]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_scan_test, host_variable_with_condition_expr) {
    // PK=(C0,C1); exact range C0=10 with condition: in[0] == p0 (both are 10).
    // Condition references an upstream stream variable and a host variable.
    // Since in[0]=10 == p0=10, all rows in the range pass the condition.
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0, 1}, {}
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

    auto input = create_nullable_record<kind::int4, kind::int4>(10, 10);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    scan_endpoint_spec lower{{0}, {in[0]}, relation::endpoint_kind::prefixed_inclusive};
    scan_endpoint_spec upper{{0}, {in[1]}, relation::endpoint_kind::prefixed_inclusive};
    auto& target = add_join_scan_node(setup, lower, upper, false,
        [&](auto const&) {
            return std::make_unique<compare>(
                comparison_operator::equal, varref{in[0]}, varref{p0});
        });
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 101), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 200), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx,
        &host_variables
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_, destinations(target.columns())));
    });

    set_variables(ex.variables_, in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 101)), result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_scan_test, multiple_types) {
    // C0=int4 (PK), C1=float8, C2=int8; exact range C0=20 returns one row.
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
    scan_endpoint_spec lower{{0}, {in[0]}, relation::endpoint_kind::prefixed_inclusive};
    scan_endpoint_spec upper{{0}, {in[0]}, relation::endpoint_kind::prefixed_inclusive};
    auto& target = add_join_scan_node(setup, lower, upper);
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::float8, kind::int8>(10, 1.0, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::float8, kind::int8>(30, 3.0, 300), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_, destinations(target.columns())));
    });

    set_variables(ex.variables_, in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_scan_test, left_outer) {
    // left_outer join: first input matches (C0=10 exists) and emits the row.
    // Second input has no match (C0=30 absent) and emits a null-padded row.
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );
    auto input_match    = create_nullable_record<kind::int4, kind::int4>(10, 10);
    auto input_no_match = create_nullable_record<kind::int4, kind::int4>(30, 30);
    auto [up, in] = add_upstream_record_provider(input_match.record_meta());
    scan_endpoint_spec lower{{0}, {in[0]}, relation::endpoint_kind::prefixed_inclusive};
    scan_endpoint_spec upper{{0}, {in[1]}, relation::endpoint_kind::prefixed_inclusive};
    auto& target = add_join_scan_node(setup, lower, upper);
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 200), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx,
        nullptr, relation::join_kind::left_outer
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_, destinations(target.columns())));
    });

    set_variables(ex.variables_, in, input_match.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 100)), result[0]);

    set_variables(ex.variables_, in, input_no_match.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(std::nullopt, std::nullopt)), result[1]);

    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_scan_test, left_outer_condition_filtered) {
    // Row is found by range scan but the condition is false.
    // With left_outer, a null-padded row is emitted instead of the scanned row.
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );
    auto input = create_nullable_record<kind::int4, kind::int4>(1, 1);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    scan_endpoint_spec lower{{0}, {in[0]}, relation::endpoint_kind::prefixed_inclusive};
    scan_endpoint_spec upper{{0}, {in[1]}, relation::endpoint_kind::prefixed_inclusive};
    // condition: C1 == 999 is false for the inserted row (C1=100)
    auto& target = add_join_scan_node(setup, lower, upper, false,
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
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx,
        nullptr, relation::join_kind::left_outer
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_, destinations(target.columns())));
    });

    set_variables(ex.variables_, in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(std::nullopt, std::nullopt)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(join_scan_test, composite_key_primary) {
    // PK=(C0,C1); scan C0=1 with prefixed_inclusive returns all rows where C0=1.
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
            {"C2", t::int4(), nullity{false}},
        }},
        {0, 1}, {}
    );
    auto input = create_nullable_record<kind::int4>(1);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    scan_endpoint_spec lower{{0}, {in[0]}, relation::endpoint_kind::prefixed_inclusive};
    scan_endpoint_spec upper{{0}, {in[0]}, relation::endpoint_kind::prefixed_inclusive};
    auto& target = add_join_scan_node(setup, lower, upper);
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 20, 200), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 300), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_, destinations(target.columns())));
    });

    set_variables(ex.variables_, in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 20, 200)), result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

/**
 * @brief Full scan: no endpoints passed (scan_endpoint_spec default = unbound).
 *
 * All rows including those with null C1 are returned in primary-key order.
 * The dummy upstream variable is killed by liveness analysis (not referenced
 * in any key expression), so set_variables must NOT be called.
 */
TEST_F(join_scan_test, full_scan) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{true}},
        }},
        {0}, {}
    );
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, std::nullopt), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 300), *db_);

    auto dummy = create_nullable_record<kind::int4>(0);
    auto [up, in] = add_upstream_record_provider(dummy.record_meta());
    auto& target = add_join_scan_node(setup, {}, {});
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_, destinations(target.columns())));
    });

    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, std::nullopt)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 300)), result[2]);
    ASSERT_EQ(status::ok, tx->commit());
}

/**
 * @brief Full scan: both endpoints explicitly constructed as unbound.
 *
 * Functionally identical to full_scan above; verifies that an explicitly
 * constructed unbound scan_endpoint_spec produces the same result.
 */
TEST_F(join_scan_test, full_scan_explicit_unbound) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{true}},
        }},
        {0}, {}
    );
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, std::nullopt), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 300), *db_);

    auto dummy = create_nullable_record<kind::int4>(0);
    auto [up, in] = add_upstream_record_provider(dummy.record_meta());
    auto& target = add_join_scan_node(
        setup, scan_endpoint_spec::unbound(), scan_endpoint_spec::unbound());
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_, destinations(target.columns())));
    });

    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, std::nullopt)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 300)), result[2]);
    ASSERT_EQ(status::ok, tx->commit());
}

/**
 * @brief Reversed bounds (lower > upper) produce an empty result.
 *
 * Both source_vars are referenced in key expressions so both survive
 * liveness analysis; set_variables works normally.
 */
TEST_F(join_scan_test, reversed_bounds) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 200), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 300), *db_);

    auto input = create_nullable_record<kind::int4, kind::int4>(30, 10);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    scan_endpoint_spec lower{{0}, {in[0]}, relation::endpoint_kind::prefixed_inclusive};
    scan_endpoint_spec upper{{0}, {in[1]}, relation::endpoint_kind::prefixed_inclusive};
    auto& target = add_join_scan_node(setup, lower, upper);
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, nullptr, down,
        get_storage(*db_, setup.primary_idx->simple_name()), nullptr, tx
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_, destinations(target.columns())));
    });

    set_variables(ex.variables_, in, input.ref());
    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(0, result.size());
    ASSERT_EQ(status::ok, tx->commit());
}

} // namespace jogasaki::executor::process::impl::ops
