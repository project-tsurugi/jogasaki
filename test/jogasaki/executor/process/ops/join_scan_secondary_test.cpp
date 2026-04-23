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
#include <vector>

#include <gtest/gtest.h>

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
using kind = field_type_kind;

/**
 * @brief Test fixture for secondary-index join_scan operator tests.
 *
 * @details Covers a two-column table T1(C0 int4 PK, C1 int4) with a secondary
 *     index on (C1, C0).  Secondary index scan returns rows in secondary-index
 *     key order: by C1 ascending (or descending), then by C0 ascending for ties.
 *
 *     Tests annotated with
 *       // TODO(fix-scan-secondary-indices): ...
 *     document correct expected behaviour that is currently broken due to the
 *     DESC-column endpoint swap bug (Problem 1 in fix-scan-secondary-indices.md).
 */
class join_scan_secondary_test : public join_scan_test_base {};

/**
 * @brief Secondary index (C1 ASC, C0 ASC); range C1 in [20,30] via
 *     prefixed_inclusive on both bounds.
 */
TEST_F(join_scan_secondary_test, secondary_index) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {1}
    );
    auto input = create_nullable_record<kind::int4, kind::int4>(20, 30);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    scan_endpoint_spec lower{{1}, {in[0]}, relation::endpoint_kind::prefixed_inclusive};
    scan_endpoint_spec upper{{1}, {in[1]}, relation::endpoint_kind::prefixed_inclusive};
    auto& target = add_join_scan_node(setup, lower, upper, true);
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(100, 10), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(200, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(201, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(300, 30), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, setup.secondary_idx.get(), down,
        get_storage(*db_, setup.primary_idx->simple_name()),
        get_storage(*db_, setup.secondary_idx->simple_name()),
        tx
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
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(200, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(201, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(300, 30)), result[2]);
    ASSERT_EQ(status::ok, tx->commit());
}

/**
 * @brief Secondary index (C1 DESC, C0 ASC); range C1 in [20,30].
 *
 * TODO(fix-scan-secondary-indices): documents correct expected behaviour;
 * currently fails because the DESC lower/upper swap is not yet implemented
 * (Problem 1 in fix-scan-secondary-indices.md).
 */
TEST_F(join_scan_secondary_test, secondary_index_desc) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {1}, {desc}
    );
    auto input = create_nullable_record<kind::int4, kind::int4>(20, 30);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    scan_endpoint_spec lower{{1}, {in[0]}, relation::endpoint_kind::prefixed_inclusive};
    scan_endpoint_spec upper{{1}, {in[1]}, relation::endpoint_kind::prefixed_inclusive};
    auto& target = add_join_scan_node(setup, lower, upper, true);
    auto verifier_vars = in.vars_;
    auto out_vars = destinations(target.columns());
    verifier_vars.insert(verifier_vars.end(), out_vars.begin(), out_vars.end());
    auto down = add_downstream_record_verifier(std::move(verifier_vars));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(100, 10), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(200, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(201, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(300, 30), *db_);

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, setup.secondary_idx.get(), down,
        get_storage(*db_, setup.primary_idx->simple_name()),
        get_storage(*db_, setup.secondary_idx->simple_name()),
        tx
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
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(200, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(201, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(300, 30)), result[2]);
    ASSERT_EQ(status::ok, tx->commit());
}

/**
 * @brief Secondary index full scan: no endpoints
 *
 * All rows including those with null C1 are returned in secondary-index order
 * (C1 ASC, then C0 ASC). Null C1 entries sort before all non-null entries.
 * The dummy upstream variable is killed by liveness analysis so set_variables
 * must NOT be called.
 */
TEST_F(join_scan_secondary_test, full_scan) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{true}},
        }},
        {0}, {1}
    );
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, std::nullopt), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);

    auto dummy = create_nullable_record<kind::int4>(0);
    auto [up, in] = add_upstream_record_provider(dummy.record_meta());
    auto& target = add_join_scan_node(setup, {}, {}, true);
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, setup.secondary_idx.get(), down,
        get_storage(*db_, setup.primary_idx->simple_name()),
        get_storage(*db_, setup.secondary_idx->simple_name()),
        tx
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_, destinations(target.columns())));
    });

    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, std::nullopt)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 30)), result[2]);
    ASSERT_EQ(status::ok, tx->commit());
}

/**
 * @brief Secondary index full scan with both endpoints explicitly unbound.
 *
 * Equivalent to full_scan above; verifies that explicitly constructed
 * unbound endpoints produce the same all-rows result.
 */
TEST_F(join_scan_secondary_test, full_scan_explicit_unbound) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{true}},
        }},
        {0}, {1}
    );
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, std::nullopt), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);

    auto dummy = create_nullable_record<kind::int4>(0);
    auto [up, in] = add_upstream_record_provider(dummy.record_meta());
    auto& target = add_join_scan_node(
        setup, scan_endpoint_spec::unbound(), scan_endpoint_spec::unbound(), true);
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, setup.secondary_idx.get(), down,
        get_storage(*db_, setup.primary_idx->simple_name()),
        get_storage(*db_, setup.secondary_idx->simple_name()),
        tx
    );

    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_, destinations(target.columns())));
    });

    ASSERT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
    ex.ctx_->release();
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, std::nullopt)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 30)), result[2]);
    ASSERT_EQ(status::ok, tx->commit());
}

/**
 * @brief Secondary index scan with lower > upper returns empty result.
 *
 * Both source_vars are referenced in key expressions so liveness analysis
 * keeps them in the variable block; set_variables works normally.
 */
TEST_F(join_scan_secondary_test, reversed_bounds) {
    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {1}
    );
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(100, 10), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(200, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(300, 30), *db_);

    auto input = create_nullable_record<kind::int4, kind::int4>(30, 10);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    scan_endpoint_spec lower{{1}, {in[0]}, relation::endpoint_kind::prefixed_inclusive};
    scan_endpoint_spec upper{{1}, {in[1]}, relation::endpoint_kind::prefixed_inclusive};
    auto& target = add_join_scan_node(setup, lower, upper, true);
    auto down = add_downstream_record_verifier(destinations(target.columns()));

    auto tx = wrap(db_->create_transaction());
    auto ex = make_join_scan_executor(
        up, target, *setup.primary_idx, setup.secondary_idx.get(), down,
        get_storage(*db_, setup.primary_idx->simple_name()),
        get_storage(*db_, setup.secondary_idx->simple_name()),
        tx
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
