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
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/relation/sort_direction.h>

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
 * @brief Test fixture for composite secondary-index join_scan operator tests.
 *
 * @details Covers a three-column table T0(C0 int4 PK, C1 int4, C2 int4) with a
 *     composite secondary index on (C1, C2).  Secondary index key is:
 *       encode(C1, C1_dir) + encode(C2, C2_dir) + encode(C0, ASC) = 12 bytes
 *
 *     Partial-key endpoints (1-col = 4 bytes, 2-col = 8 bytes) follow the same
 *     semantics as composite primary-key partial keys:
 *       - lower pre_inc: boundary entry IS included.
 *       - lower pre_exc: boundary prefix IS skipped.
 *       - upper pre_exc: boundary entry IS excluded.
 *       - upper pre_inc: all entries sharing the boundary prefix ARE included.
 *
 *     Tests annotated with
 *       // TODO(fix-scan-secondary-indices): ...
 *     document correct expected behaviour that is currently broken due to the
 *     DESC-column endpoint swap bug or the nullable upper-bound bug.
 */
class join_scan_secondary_compkey_test : public join_scan_test_base {
public:

    /**
     * @brief Value + kind specification for one scan endpoint.
     * @details col_indices and values must be the same length and at most 2.
     *     An unbound endpoint has empty col_indices and kind=unbound.
     */
    struct c_endpoint {
        std::vector<std::size_t> col_indices{};
        std::vector<std::int32_t> values{};
        relation::endpoint_kind kind{relation::endpoint_kind::unbound};

        static c_endpoint unbound() {
            return {};
        }
        bool is_unbound() const {
            return col_indices.empty() && values.empty();
        }
    };

    /**
     * @brief 1-column endpoint on C1 (table column index 1).
     * @param v  int4 value for C1.
     * @param k  endpoint kind.
     */
    static c_endpoint c1_ep(std::int32_t v, relation::endpoint_kind k) {
        return {{1}, {v}, k};
    }

    /**
     * @brief 2-column endpoint on (C1, C2) (table column indices 1, 2).
     * @param v1 int4 value for C1.
     * @param v2 int4 value for C2.
     * @param k  endpoint kind.
     */
    static c_endpoint c1c2_ep(std::int32_t v1, std::int32_t v2, relation::endpoint_kind k) {
        return {{1, 2}, {v1, v2}, k};
    }

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
     * @brief Wire, build, and execute a secondary-index join_scan returning
     *     (C0, C1, C2) records.
     *
     * @details Upstream record provides 4 int4 slots:
     *       slots 0,1  → lower endpoint values (C1, C2)
     *       slots 2,3  → upper endpoint values (C1, C2)
     *     Unused slots are set to 0 and are never referenced by any varref.
     *     Helper is intended for a single invocation per TEST_F.
     *
     * @param setup     table and index configuration.
     * @param lower_ep  lower bound specification (use c_endpoint::unbound() for
     *                  unbound).
     * @param upper_ep  upper bound specification (use c_endpoint::unbound() for
     *                  unbound).
     * @return result rows in secondary-index order.
     */
    std::vector<basic_record> run_composite_secondary_join_scan(
        table_setup const& setup,
        c_endpoint const& lower_ep,
        c_endpoint const& upper_ep
    ) {
        EXPECT_EQ(lower_ep.col_indices.size(), lower_ep.values.size());
        EXPECT_EQ(upper_ep.col_indices.size(), upper_ep.values.size());
        EXPECT_TRUE(lower_ep.values.size() <= 2);
        EXPECT_TRUE(upper_ep.values.size() <= 2);

        // 4-slot upstream record.  Liveness analysis only retains variables that
        // appear in key expressions; unused slots are "killed" and absent from the
        // variable block.  We therefore never call set_variables(); instead we
        // write key values directly into the block after the executor is built.
        auto input = create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(
            0, 0, 0, 0);
        auto [up_ref, in] = add_upstream_record_provider(input.record_meta());

        // Lower endpoint uses slots [0..n_lower-1]; upper uses slots [2..2+n_upper-1].
        scan_endpoint_spec lower_spec;
        if (! lower_ep.is_unbound()) {
            lower_spec.kind = lower_ep.kind;
            for (std::size_t i = 0; i < lower_ep.col_indices.size(); ++i) {
                lower_spec.col_indices.push_back(lower_ep.col_indices[i]);
                lower_spec.source_vars.push_back(in[i]);
            }
        }
        scan_endpoint_spec upper_spec;
        if (! upper_ep.is_unbound()) {
            upper_spec.kind = upper_ep.kind;
            for (std::size_t i = 0; i < upper_ep.col_indices.size(); ++i) {
                upper_spec.col_indices.push_back(upper_ep.col_indices[i]);
                upper_spec.source_vars.push_back(in[2 + i]);
            }
        }

        auto& target = add_join_scan_node(setup, lower_spec, upper_spec, true);
        auto down = add_downstream_record_verifier(destinations(target.columns()));
        auto tx = wrap(db_->create_transaction());
        auto ex = make_join_scan_executor(
            up_ref.get(), target,
            *setup.primary_idx, setup.secondary_idx.get(), down,
            get_storage(*db_, setup.primary_idx->simple_name()),
            get_storage(*db_, setup.secondary_idx->simple_name()),
            tx
        );

        std::vector<basic_record> result{};
        down.set_body([&]() {
            result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
        });

        // Write key values directly into the variable block.  Only variables
        // referenced in key expressions survive liveness analysis, so we only
        // touch lower_spec.source_vars and upper_spec.source_vars.
        auto dst = ex.variables_list_[0].store().ref();
        auto const& vinfo = ex.variables_list_[0].info();
        for (std::size_t i = 0; i < lower_spec.source_vars.size(); ++i) {
            auto const& vi = vinfo.at(lower_spec.source_vars[i]);
            dst.set_value<std::int32_t>(vi.value_offset(), lower_ep.values[i]);
            dst.set_null(vi.nullity_offset(), false);
        }
        for (std::size_t i = 0; i < upper_spec.source_vars.size(); ++i) {
            auto const& vi = vinfo.at(upper_spec.source_vars[i]);
            dst.set_value<std::int32_t>(vi.value_offset(), upper_ep.values[i]);
            dst.set_null(vi.nullity_offset(), false);
        }

        EXPECT_TRUE(static_cast<bool>(ex.op_(*ex.ctx_)));
        ex.ctx_->release();
        EXPECT_EQ(status::ok, tx->commit());
        return result;
    }

    /**
     * @brief Run an exact-range secondary join_scan test with nullable columns.
     *
     * @details Table has three int4 columns: C0 (primary key, non-nullable),
     *     C1 and C2 forming the composite secondary key with the given directions.
     *     When first_col_nullable is true, C1 is nullable and C2 is non-nullable;
     *     otherwise C1 is non-nullable and C2 is nullable.  A null row is inserted
     *     for the nullable column to confirm it does not appear in the results.
     *     The scan uses an exact range (C1=1, C2=2) for both lower and upper
     *     with prefixed_inclusive, relying on range bounds to exclude null rows.
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

        // Exact range: both lower and upper specify (C1=1, C2=2) with prefixed_inclusive.
        // Reusing in[0] and in[1] for both endpoints is valid: each is a separate varref
        // node that evaluates the same stream variable slot at scan time.
        scan_endpoint_spec lower{{1, 2}, {in[0], in[1]}, relation::endpoint_kind::prefixed_inclusive};
        scan_endpoint_spec upper{{1, 2}, {in[0], in[1]}, relation::endpoint_kind::prefixed_inclusive};
        auto& target = add_join_scan_node(setup, lower, upper, true);
        auto down = add_downstream_record_verifier(destinations(target.columns()));

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
        auto ex = make_join_scan_executor(
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

// ─── Exact-range tests (nullable/non-nullable columns × all direction combos) ──

TEST_F(join_scan_secondary_compkey_test, nullable_nonnullable_asc_asc) {
    do_composite_secondary_key_test(true, asc, asc);
}

TEST_F(join_scan_secondary_compkey_test, nullable_nonnullable_asc_desc) {
    do_composite_secondary_key_test(true, asc, desc);
}

TEST_F(join_scan_secondary_compkey_test, nullable_nonnullable_desc_asc) {
    do_composite_secondary_key_test(true, desc, asc);
}

TEST_F(join_scan_secondary_compkey_test, nullable_nonnullable_desc_desc) {
    do_composite_secondary_key_test(true, desc, desc);
}

TEST_F(join_scan_secondary_compkey_test, nonnullable_nullable_asc_asc) {
    do_composite_secondary_key_test(false, asc, asc);
}

TEST_F(join_scan_secondary_compkey_test, nonnullable_nullable_asc_desc) {
    do_composite_secondary_key_test(false, asc, desc);
}

TEST_F(join_scan_secondary_compkey_test, nonnullable_nullable_desc_asc) {
    do_composite_secondary_key_test(false, desc, asc);
}

TEST_F(join_scan_secondary_compkey_test, nonnullable_nullable_desc_desc) {
    do_composite_secondary_key_test(false, desc, desc);
}

// ─── Composite secondary ASC/ASC endpoint tests ────────────────────────────────

/**
 * @brief Composite secondary ASC/ASC lower=1col inclusive, upper=unbound.
 */
TEST_F(join_scan_secondary_compkey_test, asc_asc_lower_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c1_ep(20, ek::prefixed_inclusive), c_endpoint::unbound());
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[2]);
}

/**
 * @brief Composite secondary ASC/ASC lower=1col exclusive, upper=unbound.
 */
TEST_F(join_scan_secondary_compkey_test, asc_asc_lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c1_ep(20, ek::prefixed_exclusive), c_endpoint::unbound());
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[1]);
}

/**
 * @brief Composite secondary ASC/ASC lower=unbound, upper=1col inclusive.
 */
TEST_F(join_scan_secondary_compkey_test, asc_asc_upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c_endpoint::unbound(), c1_ep(30, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[2]);
}

/**
 * @brief Composite secondary ASC/ASC lower=unbound, upper=1col exclusive.
 */
TEST_F(join_scan_secondary_compkey_test, asc_asc_upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c_endpoint::unbound(), c1_ep(30, ek::prefixed_exclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[1]);
}

/**
 * @brief Composite secondary ASC/ASC lower=1col exclusive, upper=1col inclusive.
 */
TEST_F(join_scan_secondary_compkey_test, asc_asc_lower1col_upper1col_exclusive_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(
        setup, c1_ep(20, ek::prefixed_exclusive), c1_ep(30, ek::prefixed_inclusive));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
}

/**
 * @brief Composite secondary ASC/ASC lower=2col inclusive, upper=1col inclusive.
 */
TEST_F(join_scan_secondary_compkey_test, asc_asc_lower2col_upper1col_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_join_scan(
        setup, c1c2_ep(10, 20, ek::prefixed_inclusive), c1_ep(10, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[2]);
}

/**
 * @brief Composite secondary ASC/ASC lower=2col exclusive, upper=1col inclusive.
 */
TEST_F(join_scan_secondary_compkey_test, asc_asc_lower2col_upper1col_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, false);
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_join_scan(
        setup, c1c2_ep(10, 20, ek::prefixed_exclusive), c1_ep(10, ek::prefixed_inclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[1]);
}

// ─── Composite secondary ASC/DESC endpoint tests ───────────────────────────────

/**
 * @brief Composite secondary ASC/DESC lower=1col inclusive, upper=unbound.
 */
TEST_F(join_scan_secondary_compkey_test, asc_desc_lower_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c1_ep(20, ek::prefixed_inclusive), c_endpoint::unbound());
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[2]);
}

/**
 * @brief Composite secondary ASC/DESC lower=1col exclusive, upper=unbound.
 */
TEST_F(join_scan_secondary_compkey_test, asc_desc_lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c1_ep(20, ek::prefixed_exclusive), c_endpoint::unbound());
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[1]);
}

/**
 * @brief Composite secondary ASC/DESC lower=unbound, upper=1col inclusive.
 */
TEST_F(join_scan_secondary_compkey_test, asc_desc_upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c_endpoint::unbound(), c1_ep(30, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[2]);
}

/**
 * @brief Composite secondary ASC/DESC lower=unbound, upper=1col exclusive.
 */
TEST_F(join_scan_secondary_compkey_test, asc_desc_upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c_endpoint::unbound(), c1_ep(30, ek::prefixed_exclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[1]);
}

/**
 * @brief Composite secondary ASC/DESC lower=1col exclusive, upper=1col inclusive.
 */
TEST_F(join_scan_secondary_compkey_test, asc_desc_lower1col_upper1col_exclusive_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(
        setup, c1_ep(20, ek::prefixed_exclusive), c1_ep(30, ek::prefixed_inclusive));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
}

/**
 * @brief Composite secondary ASC/DESC lower=2col inclusive, upper=1col inclusive.
 *
 * C2 is DESC: within C1=10, larger C2 values sort first (C2=40, 30, 20).
 * lower=(C1=10, C2=20, prefixed_inclusive) starts at C2=20 in DESC order,
 * which is the LAST within C1=10 in byte order.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug (prefixed_inclusive lower on DESC C2 column).
 */
TEST_F(join_scan_secondary_compkey_test, asc_desc_lower2col_upper1col_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant);
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_join_scan(
        setup, c1c2_ep(10, 20, ek::prefixed_inclusive), c1_ep(10, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[2]);
}

/**
 * @brief Composite secondary ASC/DESC lower=2col exclusive, upper=1col inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug (prefixed_exclusive lower on DESC C2 column).
 */
TEST_F(join_scan_secondary_compkey_test, asc_desc_lower2col_upper1col_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::ascendant, relation::sort_direction::descendant);
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_join_scan(
        setup, c1c2_ep(10, 20, ek::prefixed_exclusive), c1_ep(10, ek::prefixed_inclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[1]);
}

// ─── Composite secondary DESC/ASC endpoint tests ───────────────────────────────

/**
 * @brief Composite secondary DESC/ASC lower=1col inclusive, upper=unbound.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug.
 */
TEST_F(join_scan_secondary_compkey_test, desc_asc_lower_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c1_ep(20, ek::prefixed_inclusive), c_endpoint::unbound());
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[2]);
}

/**
 * @brief Composite secondary DESC/ASC lower=1col exclusive, upper=unbound.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug.
 */
TEST_F(join_scan_secondary_compkey_test, desc_asc_lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c1_ep(20, ek::prefixed_exclusive), c_endpoint::unbound());
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
}

/**
 * @brief Composite secondary DESC/ASC lower=unbound, upper=1col inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug.
 */
TEST_F(join_scan_secondary_compkey_test, desc_asc_upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c_endpoint::unbound(), c1_ep(30, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[2]);
}

/**
 * @brief Composite secondary DESC/ASC lower=unbound, upper=1col exclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug.
 */
TEST_F(join_scan_secondary_compkey_test, desc_asc_upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c_endpoint::unbound(), c1_ep(30, ek::prefixed_exclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[1]);
}

/**
 * @brief Composite secondary DESC/ASC lower=1col exclusive, upper=1col inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug.
 */
TEST_F(join_scan_secondary_compkey_test, desc_asc_lower1col_upper1col_exclusive_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(
        setup, c1_ep(20, ek::prefixed_exclusive), c1_ep(30, ek::prefixed_inclusive));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
}

/**
 * @brief Composite secondary DESC/ASC lower=2col inclusive, upper=1col inclusive.
 *
 * C1 is DESC: lower and upper both use encode(C1=10,DESC) as prefix so the
 * C1 swap is not needed; C2 is ASC so this test passes without the fix.
 */
TEST_F(join_scan_secondary_compkey_test, desc_asc_lower2col_upper1col_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant);
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_join_scan(
        setup, c1c2_ep(10, 20, ek::prefixed_inclusive), c1_ep(10, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[2]);
}

/**
 * @brief Composite secondary DESC/ASC lower=2col exclusive, upper=1col inclusive.
 *
 * C1 is DESC: lower and upper both use encode(C1=10,DESC) as prefix so no swap
 * is needed; C2 is ASC so this test passes.
 */
TEST_F(join_scan_secondary_compkey_test, desc_asc_lower2col_upper1col_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::ascendant);
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_join_scan(
        setup, c1c2_ep(10, 20, ek::prefixed_exclusive), c1_ep(10, ek::prefixed_inclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[1]);
}

// ─── Composite secondary DESC/DESC endpoint tests ──────────────────────────────

/**
 * @brief Composite secondary DESC/DESC lower=1col inclusive, upper=unbound.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug.
 */
TEST_F(join_scan_secondary_compkey_test, desc_desc_lower_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c1_ep(20, ek::prefixed_inclusive), c_endpoint::unbound());
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[2]);
}

/**
 * @brief Composite secondary DESC/DESC lower=1col exclusive, upper=unbound.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug.
 */
TEST_F(join_scan_secondary_compkey_test, desc_desc_lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c1_ep(20, ek::prefixed_exclusive), c_endpoint::unbound());
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 40, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
}

/**
 * @brief Composite secondary DESC/DESC lower=unbound, upper=1col inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug.
 */
TEST_F(join_scan_secondary_compkey_test, desc_desc_upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c_endpoint::unbound(), c1_ep(30, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[2]);
}

/**
 * @brief Composite secondary DESC/DESC lower=unbound, upper=1col exclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug.
 */
TEST_F(join_scan_secondary_compkey_test, desc_desc_upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(setup, c_endpoint::unbound(), c1_ep(30, ek::prefixed_exclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 10)), result[1]);
}

/**
 * @brief Composite secondary DESC/DESC lower=1col exclusive, upper=1col inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug.
 */
TEST_F(join_scan_secondary_compkey_test, desc_desc_lower1col_upper1col_exclusive_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant);
    insert_varying_c1_rows(setup);
    auto result = run_composite_secondary_join_scan(
        setup, c1_ep(20, ek::prefixed_exclusive), c1_ep(30, ek::prefixed_inclusive));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
}

/**
 * @brief Composite secondary DESC/DESC lower=2col inclusive, upper=1col inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug.
 */
TEST_F(join_scan_secondary_compkey_test, desc_desc_lower2col_upper1col_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant);
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_join_scan(
        setup, c1c2_ep(10, 20, ek::prefixed_inclusive), c1_ep(10, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[2]);
}

/**
 * @brief Composite secondary DESC/DESC lower=2col exclusive, upper=1col inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails due to DESC endpoint bug.
 */
TEST_F(join_scan_secondary_compkey_test, desc_desc_lower2col_upper1col_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(
        false, false, relation::sort_direction::descendant, relation::sort_direction::descendant);
    insert_varying_c2_rows(setup);
    auto result = run_composite_secondary_join_scan(
        setup, c1c2_ep(10, 20, ek::prefixed_exclusive), c1_ep(10, ek::prefixed_inclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 40)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, 30)), result[1]);
}

// ─── Composite secondary nullable C1 tests ─────────────────────────────────────

/**
 * @brief Composite secondary nullable C1 full scan.
 *
 * Null C1 rows sort before non-null in ASC encoding; they appear first.
 */
TEST_F(join_scan_secondary_compkey_test, nullable_c1_full_scan) {
    auto setup = prepare_composite_secondary_table(true, false);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, std::nullopt, 10)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, std::nullopt, 40)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 40, 50)), *db_);
    auto result = run_composite_secondary_join_scan(setup, c_endpoint::unbound(), c_endpoint::unbound());
    ASSERT_EQ(5, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, std::nullopt, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, std::nullopt, 40)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[2]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[3]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 40, 50)), result[4]);
}

/**
 * @brief Composite secondary nullable C1 lower=1col exclusive, upper=unbound.
 *
 * lower=(C1=20, prefixed_exclusive) skips null rows (they precede C1=20 in ASC
 * encoding).
 */
TEST_F(join_scan_secondary_compkey_test, nullable_c1_lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(true, false);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, std::nullopt, 10)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, std::nullopt, 40)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 40, 50)), *db_);
    auto result = run_composite_secondary_join_scan(setup, c1_ep(20, ek::prefixed_exclusive), c_endpoint::unbound());
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 40, 50)), result[1]);
}

/**
 * @brief Composite secondary nullable C1 lower=unbound, upper=1col inclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails because null rows are incorrectly included in upper-bounded scans.
 */
TEST_F(join_scan_secondary_compkey_test, nullable_c1_upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(true, false);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, std::nullopt, 10)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, std::nullopt, 40)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 40, 50)), *db_);
    auto result = run_composite_secondary_join_scan(setup, c_endpoint::unbound(), c1_ep(30, ek::prefixed_inclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), result[1]);
}

/**
 * @brief Composite secondary nullable C1 lower=unbound, upper=1col exclusive.
 *
 * TODO(fix-scan-secondary-indices): expected correct behavior; currently
 * fails because null rows are incorrectly included in upper-bounded scans.
 */
TEST_F(join_scan_secondary_compkey_test, nullable_c1_upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(true, false);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, std::nullopt, 10)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, std::nullopt, 40)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 40, 50)), *db_);
    auto result = run_composite_secondary_join_scan(setup, c_endpoint::unbound(), c1_ep(30, ek::prefixed_exclusive));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 20)), result[0]);
}

// ─── Composite secondary nullable C2 tests ─────────────────────────────────────

/**
 * @brief Composite secondary nullable C2 full scan.
 *
 * Null C2 rows (within C1=10 group) sort before non-null in ASC encoding.
 */
TEST_F(join_scan_secondary_compkey_test, nullable_c2_full_scan) {
    auto setup = prepare_composite_secondary_table(false, true);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 10, 40)), *db_);
    auto result = run_composite_secondary_join_scan(setup, c_endpoint::unbound(), c_endpoint::unbound());
    ASSERT_EQ(5, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, std::nullopt)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, std::nullopt)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[2]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), result[3]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 10, 40)), result[4]);
}

/**
 * @brief Composite secondary nullable C2 lower=2col exclusive, upper=1col inclusive.
 *
 * lower=(C1=10, C2=20, prefixed_exclusive): null C2 rows (before C2=20 in byte
 * order) are excluded.
 */
TEST_F(join_scan_secondary_compkey_test, nullable_c2_lower_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, true);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 10, 40)), *db_);
    auto result = run_composite_secondary_join_scan(
        setup, c1c2_ep(10, 20, ek::prefixed_exclusive), c1_ep(10, ek::prefixed_inclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 10, 40)), result[1]);
}

/**
 * @brief Composite secondary nullable C2, lower=1col inclusive, upper=2col inclusive.
 *
 * lower=(C1=10, prefixed_inclusive): the null-indicator 0x01 byte for non-null
 * entries naturally excludes null C2 rows (null-indicator 0x00 < 0x01).
 * upper=(C1=10, C2=30, prefixed_inclusive).
 * Expected: rows with non-null C2 ≤ 30.
 */
TEST_F(join_scan_secondary_compkey_test, nullable_c2_upper_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, true);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 10, 40)), *db_);
    auto result = run_composite_secondary_join_scan(
        setup, c1_ep(10, ek::prefixed_inclusive), c1c2_ep(10, 30, ek::prefixed_inclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), result[1]);
}

/**
 * @brief Composite secondary nullable C2, lower=1col inclusive, upper=2col exclusive.
 *
 * lower=(C1=10, prefixed_inclusive): null C2 rows excluded via null-indicator byte.
 * upper=(C1=10, C2=30, prefixed_exclusive).
 * Expected: rows with non-null C2 < 30.
 */
TEST_F(join_scan_secondary_compkey_test, nullable_c2_upper_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_secondary_table(false, true);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 10, std::nullopt)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 10, 30)), *db_);
    put_row(setup, (create_nullable_record<kind::int4, kind::int4, kind::int4>(5, 10, 40)), *db_);
    auto result = run_composite_secondary_join_scan(
        setup, c1_ep(10, ek::prefixed_inclusive), c1c2_ep(10, 30, ek::prefixed_exclusive));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 10, 20)), result[0]);
}

} // namespace jogasaki::executor::process::impl::ops
