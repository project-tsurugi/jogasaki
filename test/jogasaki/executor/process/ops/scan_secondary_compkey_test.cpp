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
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/relation/sort_direction.h>
#include <takatori/scalar/immediate.h>
#include <takatori/type/primitive.h>
#include <takatori/value/primitive.h>

#include <jogasaki/executor/process/ops/scan_test_common.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace jogasaki::mock;

using yugawara::variable::nullity;

/**
 * @brief Test fixture for composite secondary-index scan operator tests.
 *
 * @details Covers a three-column table T0(C0 int4 PK, C1 int4, C2 int4) with a
 *     composite secondary index on (C1, C2).  Secondary index key is:
 *       encode(C1, C1_dir) + encode(C2, C2_dir) + encode(C0, ASC) = 12 bytes
 *
 *     Partial-key endpoints (1-col = 4 bytes, 2-col = 8 bytes) follow the same
 *     semantics as composite primary-key partial keys:
 *       - lower inc/exc/pre_inc: boundary entry IS included (12 > partial).
 *       - lower pre_exc: boundary prefix IS skipped.
 *       - upper inc/exc/pre_exc: boundary entry IS excluded (12 > partial).
 *       - upper pre_inc: all entries sharing the boundary prefix ARE included.
 *
 *     Tests that are annotated with
 *       // TODO(fix-scan-secondary-indices): ...
 *     represent correct expected behavior that is currently broken due to the
 *     DESC-column endpoint swap bug or the nullable upper-bound bug.
 */
class scan_secondary_compkey_test : public scan_test_base {
public:

    /**
     * @brief Create T0(C0 int4 PK, C1 int4, C2 int4) with composite secondary
     *     index on (C1, C2).
     */
    table_setup prepare_composite_secondary_table(
        bool c1_nullable, bool c2_nullable,
        relation::sort_direction c1_dir = relation::sort_direction::ascendant,
        relation::sort_direction c2_dir = relation::sort_direction::ascendant
    ) {
        return prepare_indices(
            {"T0",
             {
                 {"C0", t::int4(), nullity{false}},
                 {"C1", t::int4(), nullity{c1_nullable}},
                 {"C2", t::int4(), nullity{c2_nullable}},
             }},
            {0}, {1, 2}, {c1_dir, c2_dir}
        );
    }

    /**
     * @brief Insert (1,10,10),(2,20,20),(3,30,30),(4,40,40) — one distinct C1
     *     per row.
     */
    void insert_varying_c1_rows(table_setup const& setup) {
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40), *db_);
    }

    /**
     * @brief Insert (1,10,10),(2,10,20),(3,10,30),(4,10,40) — C1=10 fixed,
     *     C2 varies.
     */
    void insert_varying_c2_rows(table_setup const& setup) {
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40), *db_);
    }

    /**
     * @brief Build a 1-col scan endpoint on C1 (col index 1) with a given int4 value.
     * @param setup table and index configuration.
     * @param val   int4 value for the endpoint key.
     * @param kind  endpoint kind.
     */
    relation::scan::endpoint
    make_c1_endpoint(table_setup const& setup, std::int32_t val, relation::endpoint_kind kind) {
        namespace tv = takatori::value;
        namespace tt = takatori::type;
        return make_scan_endpoint(
            setup, {1},
            make_exprs(std::make_unique<scalar::immediate>(tv::int4(val), tt::int4())),
            kind
        );
    }

    /**
     * @brief Build a 2-col scan endpoint on (C1, C2) with given int4 values.
     * @param setup table and index configuration.
     * @param v1    int4 value for C1.
     * @param v2    int4 value for C2.
     * @param kind  endpoint kind.
     */
    relation::scan::endpoint
    make_c1c2_endpoint(
        table_setup const& setup,
        std::int32_t v1,
        std::int32_t v2,
        relation::endpoint_kind kind
    ) {
        namespace tv = takatori::value;
        namespace tt = takatori::type;
        return make_scan_endpoint(
            setup, {1, 2},
            make_exprs(
                std::make_unique<scalar::immediate>(tv::int4(v1), tt::int4()),
                std::make_unique<scalar::immediate>(tv::int4(v2), tt::int4())
            ),
            kind
        );
    }

    /**
     * @brief Wire, build, and execute a secondary-index scan returning
     *     (C0, C1, C2) records.
     * @param setup  table and index configuration.
     * @param lower  lower bound endpoint (use {} for unbound).
     * @param upper  upper bound endpoint (use {} for unbound).
     * @return result rows in secondary-index order.
     */
    std::vector<basic_record> run_composite_secondary_scan(
        table_setup const& setup,
        relation::scan::endpoint lower,
        relation::scan::endpoint upper
    ) {
        auto& target = add_scan_node(setup, true, std::move(lower), std::move(upper));
        auto down = add_downstream_record_verifier(destinations(target.columns()));
        auto tx = wrap(db_->create_transaction());
        auto ex = make_scan_executor(target, setup, true, down, tx);
        std::vector<basic_record> result{};
        down.set_body([&]() {
            result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
        });
        EXPECT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
        ex.ctx_.release();
        EXPECT_EQ(status::ok, tx->commit());
        return result;
    }
};

// Composite secondary ASC/ASC endpoint tests.

/**
 * @brief Composite secondary ASC/ASC lower=1col inclusive, upper=unbound.
 */
TEST_F(scan_secondary_compkey_test, asc_asc_lower_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, make_c1_endpoint(setup, 20, ek::prefixed_inclusive), {});
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[2]);
}

/**
 * @brief Composite secondary ASC/ASC lower=1col prefixed_exclusive,
 *     upper=unbound.
 */
TEST_F(scan_secondary_compkey_test, asc_asc_lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, make_c1_endpoint(setup, 20, ek::prefixed_exclusive), {});
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[1]);
}

/**
 * @brief Composite secondary ASC/ASC lower=unbound, upper=1col inclusive.
 */
TEST_F(scan_secondary_compkey_test, asc_asc_upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[2]);
}

/**
 * @brief Composite secondary ASC/ASC lower=unbound, upper=1col
 *     prefixed_exclusive.
 */
TEST_F(scan_secondary_compkey_test, asc_asc_upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_exclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[1]);
}

/**
 * @brief Composite secondary ASC/ASC lower=1col inclusive, upper=1col exclusive.
 */
TEST_F(scan_secondary_compkey_test, asc_asc_lower1col_upper1col_exclusive_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(
        setup, make_c1_endpoint(setup, 20, ek::prefixed_exclusive), make_c1_endpoint(setup, 30, ek::prefixed_inclusive)
    );
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
}

/**
 * @brief Composite secondary ASC/ASC lower=1col inclusive, upper=1col
 *     prefixed_inclusive.
 */
TEST_F(scan_secondary_compkey_test, asc_asc_lower2col_upper1col_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_scan(
        setup, make_c1c2_endpoint(setup, 10, 20, ek::prefixed_inclusive),
        make_c1_endpoint(setup, 10, ek::prefixed_inclusive)
    );
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[2]);
}

/**
 * @brief Composite secondary ASC/ASC lower=2col prefixed_exclusive, upper=1col
 *     prefixed_inclusive.
 */
TEST_F(scan_secondary_compkey_test, asc_asc_lower2col_upper1col_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_scan(
        setup, make_c1c2_endpoint(setup, 10, 20, ek::prefixed_exclusive),
        make_c1_endpoint(setup, 10, ek::prefixed_inclusive)
    );
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[1]);
}

/**
 * @brief Composite secondary ASC/ASC lower=2col inclusive, upper=2col exclusive.
 */
TEST_F(scan_secondary_compkey_test, asc_desc_lower_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, make_c1_endpoint(setup, 20, ek::prefixed_inclusive), {});
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[2]);
}

/**
 * @brief Composite secondary ASC/DESC lower=1col prefixed_exclusive,
 *     upper=unbound.
 */
TEST_F(scan_secondary_compkey_test, asc_desc_lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, make_c1_endpoint(setup, 20, ek::prefixed_exclusive), {});
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[1]);
}

/**
 * @brief Composite secondary ASC/DESC lower=unbound, upper=1col inclusive.
 */
TEST_F(scan_secondary_compkey_test, asc_desc_upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[2]);
}

/**
 * @brief Composite secondary ASC/DESC lower=unbound, upper=1col
 *     prefixed_exclusive.
 */
TEST_F(scan_secondary_compkey_test, asc_desc_upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_exclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[1]);
}

/**
 * @brief Composite secondary ASC/DESC lower=1col inclusive, upper=1col exclusive.
 */
TEST_F(scan_secondary_compkey_test, asc_desc_lower1col_upper1col_exclusive_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(
        setup, make_c1_endpoint(setup, 20, ek::prefixed_exclusive), make_c1_endpoint(setup, 30, ek::prefixed_inclusive)
    );
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
}

/**
 * @brief Composite secondary ASC/DESC lower=1col inclusive, upper=1col
 *     prefixed_inclusive.
 */
TEST_F(scan_secondary_compkey_test, asc_desc_lower2col_upper1col_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant
    );
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_scan(
        setup, make_c1c2_endpoint(setup, 10, 20, ek::prefixed_inclusive),
        make_c1_endpoint(setup, 10, ek::prefixed_inclusive)
    );
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[2]);
}

/**
 * @brief Composite secondary ASC/DESC lower=2col prefixed_exclusive, upper=1col
 *     prefixed_inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, asc_desc_lower2col_upper1col_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant
    );
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_scan(
        setup, make_c1c2_endpoint(setup, 10, 20, ek::prefixed_exclusive),
        make_c1_endpoint(setup, 10, ek::prefixed_inclusive)
    );
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[1]);
}

/**
 * @brief Composite secondary ASC/DESC lower=2col inclusive, upper=2col exclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, desc_asc_lower_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, make_c1_endpoint(setup, 20, ek::prefixed_inclusive), {});
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[2]);
}

/**
 * @brief Composite secondary DESC/ASC lower=1col prefixed_exclusive,
 *     upper=unbound.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, desc_asc_lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, make_c1_endpoint(setup, 20, ek::prefixed_exclusive), {});
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
}

/**
 * @brief Composite secondary DESC/ASC lower=unbound, upper=1col inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, desc_asc_upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[2]);
}

/**
 * @brief Composite secondary DESC/ASC lower=unbound, upper=1col
 *     prefixed_exclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, desc_asc_upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_exclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[1]);
}

/**
 * @brief Composite secondary DESC/ASC lower=1col inclusive, upper=1col exclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, desc_asc_lower1col_upper1col_exclusive_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(
        setup, make_c1_endpoint(setup, 20, ek::prefixed_exclusive), make_c1_endpoint(setup, 30, ek::prefixed_inclusive)
    );
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
}

/**
 * @brief Composite secondary DESC/ASC lower=1col inclusive, upper=1col
 *     prefixed_inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, desc_asc_lower2col_upper1col_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant
    );
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_scan(
        setup, make_c1c2_endpoint(setup, 10, 20, ek::prefixed_inclusive),
        make_c1_endpoint(setup, 10, ek::prefixed_inclusive)
    );
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[2]);
}

/**
 * @brief Composite secondary DESC/ASC lower=2col prefixed_exclusive, upper=1col
 *     prefixed_inclusive.
 */
TEST_F(scan_secondary_compkey_test, desc_asc_lower2col_upper1col_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant
    );
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_scan(
        setup, make_c1c2_endpoint(setup, 10, 20, ek::prefixed_exclusive),
        make_c1_endpoint(setup, 10, ek::prefixed_inclusive)
    );
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[1]);
}

/**
 * @brief Composite secondary DESC/ASC lower=2col inclusive, upper=2col exclusive.
 */
TEST_F(scan_secondary_compkey_test, desc_desc_lower_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, make_c1_endpoint(setup, 20, ek::prefixed_inclusive), {});
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[2]);
}

/**
 * @brief Composite secondary DESC/DESC lower=1col prefixed_exclusive,
 *     upper=unbound.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, desc_desc_lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, make_c1_endpoint(setup, 20, ek::prefixed_exclusive), {});
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
}

/**
 * @brief Composite secondary DESC/DESC lower=unbound, upper=1col inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, desc_desc_upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[2]);
}

/**
 * @brief Composite secondary DESC/DESC lower=unbound, upper=1col
 *     prefixed_exclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, desc_desc_upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_exclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[1]);
}

/**
 * @brief Composite secondary DESC/DESC lower=1col inclusive, upper=1col
 *     exclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, desc_desc_lower1col_upper1col_exclusive_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant
    );
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_scan(
        setup, make_c1_endpoint(setup, 20, ek::prefixed_exclusive), make_c1_endpoint(setup, 30, ek::prefixed_inclusive)
    );
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
}

/**
 * @brief Composite secondary DESC/DESC lower=1col inclusive, upper=1col
 *     prefixed_inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, desc_desc_lower2col_upper1col_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant
    );
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_scan(
        setup, make_c1c2_endpoint(setup, 10, 20, ek::prefixed_inclusive),
        make_c1_endpoint(setup, 10, ek::prefixed_inclusive)
    );
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[2]);
}

/**
 * @brief Composite secondary DESC/DESC lower=2col prefixed_exclusive, upper=1col
 *     prefixed_inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, desc_desc_lower2col_upper1col_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant
    );
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_scan(
        setup, make_c1c2_endpoint(setup, 10, 20, ek::prefixed_exclusive),
        make_c1_endpoint(setup, 10, ek::prefixed_inclusive)
    );
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[1]);
}

/**
 * @brief Composite secondary DESC/DESC lower=2col inclusive, upper=2col
 *     exclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug
 */
TEST_F(scan_secondary_compkey_test, nullable_c1_full_scan) {
    auto setup = prepare_composite_secondary_table(true, false);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, std::nullopt, 10)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, std::nullopt, 40)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 40, 50)), *db_);
    auto result = run_composite_secondary_scan(setup, {}, {});
    ASSERT_EQ(5, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, std::nullopt, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, std::nullopt, 40)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[2]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[3]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 40, 50)), result[4]);
}

/**
 * @brief Composite secondary nullable C1 lower=1col inclusive, upper=unbound.
 */
TEST_F(scan_secondary_compkey_test, nullable_c1_lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(true, false);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, std::nullopt, 10)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, std::nullopt, 40)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 40, 50)), *db_);
    auto result = run_composite_secondary_scan(setup, make_c1_endpoint(setup, 20, ek::prefixed_exclusive), {});
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 40, 50)), result[1]);
}

/**
 * @brief Composite secondary nullable C1 lower=unbound, upper=1col inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails because null rows are incorrectly included in upper-bounded
 * scans
 */
TEST_F(scan_secondary_compkey_test, nullable_c1_upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(true, false);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, std::nullopt, 10)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, std::nullopt, 40)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 40, 50)), *db_);
    auto result = run_composite_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_inclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
}

/**
 * @brief Composite secondary nullable C1 lower=unbound, upper=1col
 *     prefixed_exclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails because null rows are incorrectly included in upper-bounded
 * scans
 */
TEST_F(scan_secondary_compkey_test, nullable_c1_upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(true, false);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, std::nullopt, 10)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, std::nullopt, 40)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 40, 50)), *db_);
    auto result = run_composite_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_exclusive));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[0]);
}

// Composite secondary nullable-C2 endpoint tests.

/**
 * @brief Composite secondary nullable C2 full scan.
 */
TEST_F(scan_secondary_compkey_test, nullable_c2_full_scan) {
    auto setup = prepare_composite_secondary_table(false, true);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 10, 40)), *db_);
    auto result = run_composite_secondary_scan(setup, {}, {});
    ASSERT_EQ(5, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, std::nullopt)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, std::nullopt)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[2]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), result[3]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 10, 40)), result[4]);
}

/**
 * @brief Composite secondary nullable C2 lower=2col inclusive, upper=unbound.
 */
TEST_F(scan_secondary_compkey_test, nullable_c2_lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, true);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 10, 40)), *db_);
    // upper=(C1=10, prefixed_inclusive) uses n_diff=1 (planner premise)
    auto result = run_composite_secondary_scan(setup, make_c1c2_endpoint(setup, 10, 20, ek::prefixed_exclusive),
        make_c1_endpoint(setup, 10, ek::prefixed_inclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 10, 40)), result[1]);
}

/**
 * @brief Composite secondary nullable C2, lower=1col, upper=2col prefixed_inclusive.
 *
 * lower=(C1=10, prefixed_inclusive) excludes null C2 entries via null-indicator begin byte.
 * upper=(C1=10, C2=30, prefixed_inclusive). Expected: rows with C2 in (non-null, ≤ 30).
 */
TEST_F(scan_secondary_compkey_test, nullable_c2_upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, true);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 10, 40)), *db_);
    // lower=(C1=10, prefixed_inclusive) uses n_diff=1 (planner premise)
    auto result = run_composite_secondary_scan(setup, make_c1_endpoint(setup, 10, ek::prefixed_inclusive),
        make_c1c2_endpoint(setup, 10, 30, ek::prefixed_inclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), result[1]);
}

/**
 * @brief Composite secondary nullable C2, lower=1col, upper=2col prefixed_exclusive.
 *
 * lower=(C1=10, prefixed_inclusive) excludes null C2 entries via null-indicator begin byte.
 * upper=(C1=10, C2=30, prefixed_exclusive). Expected: rows with C2 in (non-null, < 30).
 */
TEST_F(scan_secondary_compkey_test, nullable_c2_upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, true);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 10, 40)), *db_);
    // lower=(C1=10, prefixed_inclusive) uses n_diff=1 (planner premise)
    auto result = run_composite_secondary_scan(setup, make_c1_endpoint(setup, 10, ek::prefixed_inclusive),
        make_c1c2_endpoint(setup, 10, 30, ek::prefixed_exclusive));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[0]);
}

}  // namespace jogasaki::executor::process::impl::ops
