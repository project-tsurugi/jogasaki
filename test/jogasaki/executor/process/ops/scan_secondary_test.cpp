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
#include <memory>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

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
 * @brief Test fixture for secondary-index scan operator tests.
 *
 * @details Helpers use a two-column table T0(C0 int4 PK, C1 int4 secondary).
 *     Secondary index scan returns rows in secondary-index key order:
 *     by C1 ascending, then by C0 ascending for ties.
 *
 *     Null encoding for nullable columns: a null-indicator byte is prepended to
 *     the encoded value.  Null is encoded as 0x00, non-null as 0x01.  In ascending
 *     order NULL < non-null, so null entries sort before any non-null entries.
 *     Consequently:
 *       - Lower-bounded scans exclude null rows (they fall below the lower bound).
 *       - Upper-bounded scans include null rows (they fall below the upper bound).
 *       - Full scans include all rows; null rows appear first.
 */
class scan_secondary_test : public scan_test_base {
public:

    /**
     * @brief Create T0(C0 int4 PK, C1 int4 secondary) with the given nullity for C1.
     * @param c1_nullable whether C1 is nullable.
     */
    table_setup prepare_secondary_table(bool c1_nullable) {
        return prepare_indices(
            {"T0",
             {
                 {"C0", t::int4(), nullity{false}},
                 {"C1", t::int4(), nullity{c1_nullable}},
             }},
            {0}, {1}
        );
    }

    /**
     * @brief Build a scan endpoint on column C1 (col index 1) with a given int4 value.
     * @param setup table and index configuration.
     * @param val   int4 value for the endpoint key.
     * @param kind  endpoint kind.
     */
    relation::scan::endpoint
    make_c1_endpoint(table_setup const& setup, std::int32_t val, relation::endpoint_kind kind) {
        namespace tv = takatori::value;
        namespace tt = takatori::type;
        return make_scan_endpoint(
            setup, {1}, make_exprs(std::make_unique<scalar::immediate>(tv::int4(val), tt::int4())), kind
        );
    }

    /**
     * @brief Wire, build, and execute a secondary-index scan returning (C0, C1) records.
     * @param setup  table and index configuration.
     * @param lower  lower bound endpoint (use {} for unbound).
     * @param upper  upper bound endpoint (use {} for unbound).
     * @return result rows in secondary-index order.
     */
    std::vector<basic_record>
    run_secondary_scan(table_setup const& setup, relation::scan::endpoint lower, relation::scan::endpoint upper) {
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

TEST_F(scan_secondary_test, simple) {
    namespace tv = takatori::value;
    namespace tt = takatori::type;
    using ek = relation::endpoint_kind;

    auto setup = prepare_indices(
        {"T0",
         {
             {"C0", t::int4(), nullity{false}},
             {"C1", t::int4(), nullity{false}},
         }},
        {0}, {1}
    );
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 200), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(21, 201), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 300), *db_);

    auto& target = add_scan_node(
        setup, true,
        make_scan_endpoint(
            setup, {1}, make_exprs(std::make_unique<scalar::immediate>(tv::int4(100), tt::int4())), ek::prefixed_exclusive
        ),
        make_scan_endpoint(
            setup, {1}, make_exprs(std::make_unique<scalar::immediate>(tv::int4(300), tt::int4())), ek::prefixed_exclusive
        )
    );
    auto out = create_nullable_record<kind::int4, kind::int4>();
    auto down = add_downstream_record_verifier(destinations(target.columns()));
    auto tx = wrap(db_->create_transaction());
    auto ex = make_scan_executor(target, setup, true, down, tx);
    std::vector<basic_record> result{};
    down.set_body([&]() { result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns()))); });
    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 200)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(21, 201)), result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(scan_secondary_test, lower_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_secondary_table(false);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 10), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(40, 40), *db_);

    auto result = run_secondary_scan(setup, make_c1_endpoint(setup, 20, ek::prefixed_inclusive), {});
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(40, 40)), result[2]);
}

TEST_F(scan_secondary_test, lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_secondary_table(false);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 10), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(40, 40), *db_);

    auto result = run_secondary_scan(setup, make_c1_endpoint(setup, 20, ek::prefixed_exclusive), {});
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(40, 40)), result[1]);
}

TEST_F(scan_secondary_test, upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_secondary_table(false);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 10), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(40, 40), *db_);

    auto result = run_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 30)), result[2]);
}

TEST_F(scan_secondary_test, upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_secondary_table(false);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 10), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(40, 40), *db_);

    auto result = run_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_exclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 20)), result[1]);
}

// Nullable single-column secondary index endpoint tests.
// Table: T0(C0 int4 PK, C1 int4 secondary nullable)
// Data:  (10,null),(20,20),(30,30),(40,40),(50,null)
//
// Secondary index order: (10,null),(50,null),(20,20),(30,30),(40,40)
//
// Lower-bounded scans: null entries are excluded (correct behavior).
// Upper-bounded scans: null entries should be excluded; they are currently
//   returned due to null-indicator encoding (0x00 < 0x01 in ascending order),
//   which is a known bug (TODO: fix).  The tests below reflect the intended
//   (correct) behavior and therefore fail until the bug is fixed.
// Full scan: all rows returned, null rows appear first.

TEST_F(scan_secondary_test, nullable_full_scan) {
    auto setup = prepare_secondary_table(true);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, std::nullopt), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(40, 40), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(50, std::nullopt), *db_);

    auto result = run_secondary_scan(setup, {}, {});
    ASSERT_EQ(5, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, std::nullopt)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(50, std::nullopt)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 20)), result[2]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 30)), result[3]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(40, 40)), result[4]);
}

TEST_F(scan_secondary_test, nullable_lower_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_secondary_table(true);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, std::nullopt), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(40, 40), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(50, std::nullopt), *db_);

    auto result = run_secondary_scan(setup, make_c1_endpoint(setup, 20, ek::prefixed_inclusive), {});
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(40, 40)), result[2]);
}

TEST_F(scan_secondary_test, nullable_lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_secondary_table(true);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, std::nullopt), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(40, 40), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(50, std::nullopt), *db_);

    auto result = run_secondary_scan(setup, make_c1_endpoint(setup, 20, ek::prefixed_exclusive), {});
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(40, 40)), result[1]);
}

TEST_F(scan_secondary_test, nullable_upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_secondary_table(true);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, std::nullopt), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(40, 40), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(50, std::nullopt), *db_);

    auto result = run_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_inclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 30)), result[1]);
}

TEST_F(scan_secondary_test, nullable_upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_secondary_table(true);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, std::nullopt), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(40, 40), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(50, std::nullopt), *db_);

    auto result = run_secondary_scan(setup, {}, make_c1_endpoint(setup, 30, ek::prefixed_exclusive));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 20)), result[0]);
}

/**
 * @brief Secondary index full scan with no endpoints
 *
 * All rows are returned in secondary-index (C1 ASC, C0 ASC) order.
 * The data contains only non-nullable values to keep the ordering simple.
 */
TEST_F(scan_secondary_test, full_scan_no_endpoints) {
    auto setup = prepare_secondary_table(false);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 10), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);

    auto& target = add_scan_node(setup, true);
    auto out = create_nullable_record<kind::int4, kind::int4>();
    auto down = add_downstream_record_verifier(destinations(target.columns()));
    auto tx = wrap(db_->create_transaction());
    auto ex = make_scan_executor(target, setup, true, down, tx);
    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });
    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 30)), result[2]);
    ASSERT_EQ(status::ok, tx->commit());
}

/**
 * @brief Secondary index full scan with both endpoints passed as explicit
 *     unbound
 *
 * Null C1 rows appear first in ASC secondary-index order (null < non-null).
 */
TEST_F(scan_secondary_test, full_scan_explicit_unbound) {
    auto setup = prepare_secondary_table(true);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, std::nullopt), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);

    auto result = run_secondary_scan(setup, {}, {});
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, std::nullopt)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 30)), result[2]);
}

/**
 * @brief Secondary index scan with lower > upper returns empty result.
 */
TEST_F(scan_secondary_test, reversed_bounds) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_secondary_table(false);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 10), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 20), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 30), *db_);

    auto result = run_secondary_scan(
        setup,
        make_c1_endpoint(setup, 30, ek::prefixed_inclusive),
        make_c1_endpoint(setup, 10, ek::prefixed_inclusive));
    ASSERT_EQ(0, result.size());
}

}  // namespace jogasaki::executor::process::impl::ops
