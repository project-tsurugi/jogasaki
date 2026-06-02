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
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/scalar/immediate.h>
#include <takatori/scalar/variable_reference.h>
#include <takatori/type/character.h>
#include <takatori/type/primitive.h>
#include <takatori/type/varying.h>
#include <takatori/value/character.h>
#include <takatori/value/primitive.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/kvsservice/transaction_option.h>
#include <jogasaki/api/kvsservice/transaction_type.h>
#include <jogasaki/api/transaction_option.h>
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
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

using yugawara::variable::nullity;

class scan_test : public scan_test_base {
public:
    /**
     * @brief Set up and run a scan-ranges RTX test.
     *
     * @details Creates a standard 3-column table with composite primary key,
     *     inserts a scan node with fixed bounded key ranges, then builds scan
     *     ranges with an RTX transaction and passes them to the check function.
     *
     * @param default_parallel  value for configuration scan_default_parallel
     * @param opt               transaction options for the RTX transaction
     * @param check             callback receiving the generated scan range vector
     */
    void do_rtx_range_test(
        int default_parallel,
        std::shared_ptr<api::transaction_option> opt,
        std::function<void(std::vector<std::shared_ptr<impl::scan_range>> const&)> const& check
    ) {
        auto cfg = std::make_shared<configuration>();
        cfg->scan_default_parallel(default_parallel);
        cfg->key_distribution(key_distribution_kind::simple);
        global::config_pool(cfg);

        auto setup = prepare_indices(
            {"T0", {
                {"C0", t::int4(), nullity{false}},
                {"C1", t::int8(), nullity{false}},
                {"C2", t::int8(), nullity{false}},
            }},
            {0, 1}, {}
        );

        using ek = relation::endpoint_kind;
        namespace tv = takatori::value;
        namespace tt = takatori::type;
        auto& target = add_scan_node(
            setup,
            false,
            make_scan_endpoint(setup, {0, 1}, make_exprs(
                std::make_unique<scalar::immediate>(tv::int4(100), tt::int4()),
                std::make_unique<scalar::immediate>(tv::int8(200), tt::int8())
            ), ek::prefixed_exclusive),
            make_scan_endpoint(setup, {0, 1}, make_exprs(
                std::make_unique<scalar::immediate>(tv::int4(251658240), tt::int4()),
                std::make_unique<scalar::immediate>(tv::int8(INT64_MIN), tt::int8())
            ), ek::prefixed_exclusive)
        );

        auto down = add_downstream_record_verifier(destinations(target.columns()));
        target.output() >> down.input();
        create_processor_info();

        std::shared_ptr<kvs::transaction> tra;
        auto transaction_ctx = std::make_shared<transaction_context>(tra, std::move(opt));
        transaction_ctx->error_info(create_error_info(error_code::none, "", status::err_unknown));
        request_context_.transaction(transaction_ctx);

        io_exchange_map exchange_map{};
        operator_builder builder{processor_info_, {}, {}, exchange_map, &request_context_};
        check(builder.create_scan_ranges(target));
    }

    /**
     * @brief Create a two-column table with a single int4 primary key (C0) and an int4 value (C1).
     * @return the table_setup for the created table and primary index.
     */
    table_setup prepare_single_col_pk_table() {
        return prepare_indices(
            {"T0", {
                {"C0", t::int4(), nullity{false}},
                {"C1", t::int4(), nullity{false}},
            }},
            {0}, {}
        );
    }

    /**
     * @brief Insert rows (10,1),(20,2),(30,3),(40,4) into the given setup.
     * @param setup table_setup to insert into.
     */
    void insert_endpoint_test_rows(table_setup const& setup) {
        put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 1), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 2), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 3), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4>(40, 4), *db_);
    }

    /**
     * @brief Build a scan endpoint for column 0 (int4) with a single immediate value.
     * @param setup table and index configuration.
     * @param val   endpoint key value.
     * @param kind  endpoint kind.
     * @return the scan endpoint.
     */
    relation::scan::endpoint make_int4_endpoint(
        table_setup const& setup,
        std::int32_t val,
        relation::endpoint_kind kind
    ) {
        namespace tv = takatori::value;
        namespace tt = takatori::type;
        return make_scan_endpoint(setup, {0}, make_exprs(
            std::make_unique<scalar::immediate>(tv::int4(val), tt::int4())
        ), kind);
    }

    /**
     * @brief Wire, build, and execute a primary-index scan on setup with the given endpoints.
     * @param setup  table and index configuration.
     * @param lower  lower bound endpoint (use {} for unbound).
     * @param upper  upper bound endpoint (use {} for unbound).
     * @return the result rows from the scan.
     */
    std::vector<basic_record> run_single_col_pk_scan(
        table_setup const& setup,
        relation::scan::endpoint lower,
        relation::scan::endpoint upper
    ) {
        auto& target = add_scan_node(setup, false, std::move(lower), std::move(upper));
        auto out = create_nullable_record<kind::int4, kind::int4>();
        auto down = add_downstream_record_verifier(destinations(target.columns()));
        auto tx = wrap(db_->create_transaction());
        auto ex = make_scan_executor(target, setup, false, down, out, tx);
        std::vector<basic_record> result{};
        down.set_body([&]() {
            result.emplace_back(
                get_variables(ex.variables_list_[0], destinations(target.columns())));
        });
        EXPECT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
        ex.ctx_.release();
        EXPECT_EQ(status::ok, tx->commit());
        return result;
    }

    /**
     * @brief Create a three-column table with composite int4 primary key (C0, C1) and int4 value column (C2).
     * @return the table_setup for the created table and primary index.
     */
    table_setup prepare_composite_pk_table() {
        return prepare_indices(
            {"T0", {
                {"C0", t::int4(), nullity{false}},
                {"C1", t::int4(), nullity{false}},
                {"C2", t::int4(), nullity{false}},
            }},
            {0, 1}, {}
        );
    }

    /**
     * @brief Build a scan endpoint for columns C0 and C1 (both int4) with immediate values.
     * @param setup  table and index configuration.
     * @param v0     value for column C0.
     * @param v1     value for column C1.
     * @param kind   endpoint kind.
     * @return the scan endpoint.
     */
    relation::scan::endpoint make_int4_2col_endpoint(
        table_setup const& setup,
        std::int32_t v0,
        std::int32_t v1,
        relation::endpoint_kind kind
    ) {
        namespace tv = takatori::value;
        namespace tt = takatori::type;
        return make_scan_endpoint(setup, {0, 1}, make_exprs(
            std::make_unique<scalar::immediate>(tv::int4(v0), tt::int4()),
            std::make_unique<scalar::immediate>(tv::int4(v1), tt::int4())
        ), kind);
    }

    /**
     * @brief Insert rows with distinct C0 values into the composite PK setup.
     * @details Inserts (C0=10,C1=10,C2=1),(C0=20,C1=10,C2=2),(C0=30,C1=10,C2=3),(C0=40,C1=10,C2=4).
     * @param setup table_setup to insert into.
     */
    void insert_composite_pk_c0_rows(table_setup const& setup) {
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 10, 1), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(20, 10, 2), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(30, 10, 3), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(40, 10, 4), *db_);
    }

    /**
     * @brief Insert rows with distinct C1 values (C0 fixed at 10) into the composite PK setup.
     * @details Inserts (C0=10,C1=10,C2=1),(C0=10,C1=20,C2=2),(C0=10,C1=30,C2=3),(C0=10,C1=40,C2=4).
     * @param setup table_setup to insert into.
     */
    void insert_composite_pk_c1_rows(table_setup const& setup) {
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 10, 1), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 20, 2), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 30, 3), *db_);
        put_row(setup, create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 40, 4), *db_);
    }

    /**
     * @brief Wire, build, and execute a primary-index scan on the composite PK setup with the given endpoints.
     * @param setup  table and index configuration.
     * @param lower  lower bound endpoint (use {} for unbound).
     * @param upper  upper bound endpoint (use {} for unbound).
     * @return the result rows (C0, C1, C2) from the scan.
     */
    std::vector<basic_record> run_composite_pk_scan(
        table_setup const& setup,
        relation::scan::endpoint lower,
        relation::scan::endpoint upper
    ) {
        auto& target = add_scan_node(setup, false, std::move(lower), std::move(upper));
        auto out = create_nullable_record<kind::int4, kind::int4, kind::int4>();
        auto down = add_downstream_record_verifier(destinations(target.columns()));
        auto tx = wrap(db_->create_transaction());
        auto ex = make_scan_executor(target, setup, false, down, out, tx);
        std::vector<basic_record> result{};
        down.set_body([&]() {
            result.emplace_back(
                get_variables(ex.variables_list_[0], destinations(target.columns())));
        });
        EXPECT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
        ex.ctx_.release();
        EXPECT_EQ(status::ok, tx->commit());
        return result;
    }
};

TEST_F(scan_test, simple) {
    auto setup = prepare_indices(
        {"T0", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 200), *db_);

    auto& target = add_scan_node(setup);
    auto out = create_nullable_record<kind::int4, kind::int4>();
    auto down = add_downstream_record_verifier(destinations(target.columns()));
    auto tx = wrap(db_->create_transaction());
    auto ex = make_scan_executor(target, setup, false, down, out, tx);
    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });
    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 200)), result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(scan_test, nullable_fields) {
    auto setup = prepare_indices(
        {"T0", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{true}},
        }},
        {0}, {}
    );
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, std::nullopt), *db_);

    auto& target = add_scan_node(setup);
    auto out = create_nullable_record<kind::int4, kind::int4>();
    auto down = add_downstream_record_verifier(destinations(target.columns()));
    auto tx = wrap(db_->create_transaction());
    auto ex = make_scan_executor(target, setup, false, down, out, tx);
    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(
            get_variables(ex.variables_list_[0], destinations(target.columns())));
    });
    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, std::nullopt)), result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(scan_test, scan_info) {
    namespace tv = takatori::value;
    namespace tt = takatori::type;
    using ek = relation::endpoint_kind;

    auto setup = prepare_indices(
        {"T0", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );

    auto& target = add_scan_node(setup, false,
        make_scan_endpoint(setup, {0}, make_exprs(
            std::make_unique<scalar::immediate>(tv::int4(10), tt::int4())
        ), ek::prefixed_exclusive),
        make_scan_endpoint(setup, {0}, make_exprs(
            std::make_unique<scalar::immediate>(tv::int4(40), tt::int4())
        ), ek::prefixed_exclusive));

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 1), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 2), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 3), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(40, 4), *db_);

    auto out = create_nullable_record<kind::int4, kind::int4>();
    auto down = add_downstream_record_verifier(destinations(target.columns()));
    auto tx = wrap(db_->create_transaction());
    auto ex = make_scan_executor(target, setup, false, down, out, tx);
    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(
            get_variables(ex.variables_list_[0], destinations(target.columns())));
    });
    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 2)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 3)), result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(scan_test, multiple_types) {
    namespace tv = takatori::value;
    namespace tt = takatori::type;
    using ek = relation::endpoint_kind;

    auto setup = prepare_indices(
        {"T1", {
            {"C0", t::int8(), nullity{false}},
            {"C1", t::character(t::varying, 100), nullity{false}},
            {"C2", t::float8(), nullity{false}},
        }},
        {0, 1}, {}
    );

    auto& target = add_scan_node(setup, false,
        make_scan_endpoint(setup, {0, 1}, make_exprs(
            std::make_unique<scalar::immediate>(tv::int8(100), tt::int8()),
            std::make_unique<scalar::immediate>(
                tv::character("123456789012345678901234567890/B"), tt::character(tt::varying, 100))
        ), ek::prefixed_inclusive),
        make_scan_endpoint(setup, {0, 1}, make_exprs(
            std::make_unique<scalar::immediate>(tv::int8(100), tt::int8()),
            std::make_unique<scalar::immediate>(
                tv::character("123456789012345678901234567890/D"), tt::character(tt::varying, 100))
        ), ek::prefixed_exclusive));

    put_row(setup,
        create_nullable_record<kind::int8, kind::character, kind::float8>(
            100, accessor::text{"123456789012345678901234567890/B"}, 1.0),
        *db_);
    put_row(setup,
        create_nullable_record<kind::int8, kind::character, kind::float8>(
            100, accessor::text{"123456789012345678901234567890/C"}, 2.0),
        *db_);
    put_row(setup,
        create_nullable_record<kind::int8, kind::character, kind::float8>(
            100, accessor::text{"123456789012345678901234567890/D"}, 3.0),
        *db_);

    auto out = create_nullable_record<kind::int8, kind::character, kind::float8>();
    auto down = add_downstream_record_verifier(destinations(target.columns()));
    auto tx = wrap(db_->create_transaction());
    auto ex = make_scan_executor(target, setup, false, down, out, tx);
    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(
            get_variables(ex.variables_list_[0], destinations(target.columns())));
    });
    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::character, kind::float8>(
        100, accessor::text("123456789012345678901234567890/B"), 1.0)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::character, kind::float8>(
        100, accessor::text("123456789012345678901234567890/C"), 2.0)), result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(scan_test, host_variables) {
    auto setup = prepare_indices(
        {"T0", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );

    auto host_variable_record = create_nullable_record<kind::int4, kind::int4>(10, 30);
    auto p_lo = bindings_(register_variable("p_lo", kind::int4));
    auto p_hi = bindings_(register_variable("p_hi", kind::int4));
    variable_table_info host_variable_info{
        std::unordered_map<variable, std::size_t>{{p_lo, 0}, {p_hi, 1}},
        std::unordered_map<std::string, takatori::descriptor::variable>{
            {"p_lo", p_lo}, {"p_hi", p_hi}},
        host_variable_record.record_meta()
    };
    variable_table host_variables{host_variable_info};
    host_variables.store().set(host_variable_record.ref());

    using ek = relation::endpoint_kind;
    auto& target = add_scan_node(
        setup,
        false,
        make_scan_endpoint(setup, {0}, make_exprs(
            std::make_unique<scalar::variable_reference>(p_lo)
        ), ek::prefixed_exclusive),
        make_scan_endpoint(setup, {0}, make_exprs(
            std::make_unique<scalar::variable_reference>(p_hi)
        ), ek::prefixed_exclusive)
    );

    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 1), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 2), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 3), *db_);

    auto out = create_nullable_record<kind::int4, kind::int4>();
    auto down = add_downstream_record_verifier(destinations(target.columns()));
    auto tx = wrap(db_->create_transaction());
    auto ex = make_scan_executor(target, setup, false, down, out, tx, &host_variables);
    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(
            get_variables(ex.variables_list_[0], destinations(target.columns())));
    });
    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 2)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

/**
 * @brief Scan information test for #1180 (RTX scan with parallelism of 1)
 *
 * Ensures that the start and end endpoints of the range scan are exclusive.
 */
TEST_F(scan_test, scan_info_rtx_parallel_1) {
    // issues #1180
    do_rtx_range_test(
        1,
        std::make_shared<api::transaction_option>(
            transaction_type_kind::rtx,
            std::vector<std::string>{"table1", "table2"},
            "",
            std::vector<std::string>{"area1", "area2"},
            std::vector<std::string>{"area3", "area4"}),
        [](auto const& ranges) {
            ASSERT_EQ(1, ranges.size());
            EXPECT_EQ(ranges[0]->begin().endpointkind(), kvs::end_point_kind::prefixed_exclusive);
            EXPECT_EQ(ranges[0]->end().endpointkind(), kvs::end_point_kind::prefixed_exclusive);
        });
}

/**
 * @brief Scan information test for #1180 (RTX scan with parallelism of 2)
 *
 * Ensures that the first range starts exclusive and the second ends exclusive with
 * an inclusive start.
 */
TEST_F(scan_test, scan_info_rtx_parallel_2) {
    // issues #1180
    do_rtx_range_test(
        2,
        std::make_shared<api::transaction_option>(
            transaction_type_kind::rtx,
            std::vector<std::string>{"table1", "table2"},
            "",
            std::vector<std::string>{"area1", "area2"},
            std::vector<std::string>{"area3", "area4"}),
        [](auto const& ranges) {
            ASSERT_EQ(2, ranges.size());
            EXPECT_EQ(ranges[0]->begin().endpointkind(), kvs::end_point_kind::prefixed_exclusive);
            EXPECT_EQ(ranges[0]->end().endpointkind(), kvs::end_point_kind::exclusive);
            EXPECT_EQ(ranges[1]->begin().endpointkind(), kvs::end_point_kind::inclusive);
            EXPECT_EQ(ranges[1]->end().endpointkind(), kvs::end_point_kind::prefixed_exclusive);
        });
}

/**
 * @brief Scan information test for #1180 (RTX scan with parallelism of 4)
 *
 * Ensures that scan ranges are properly divided into 4 parts with correct endpoint kinds.
 */
TEST_F(scan_test, scan_info_rtx_parallel_4) {
    // issues #1180
    do_rtx_range_test(
        4,
        std::make_shared<api::transaction_option>(
            transaction_type_kind::rtx,
            std::vector<std::string>{"table1", "table2"},
            "",
            std::vector<std::string>{"area1", "area2"},
            std::vector<std::string>{"area3", "area4"}),
        [](auto const& ranges) {
            ASSERT_EQ(4, ranges.size());
            EXPECT_EQ(ranges[0]->begin().endpointkind(), kvs::end_point_kind::prefixed_exclusive);
            EXPECT_EQ(ranges[0]->end().endpointkind(), kvs::end_point_kind::exclusive);
            EXPECT_EQ(ranges[1]->begin().endpointkind(), kvs::end_point_kind::inclusive);
            EXPECT_EQ(ranges[1]->end().endpointkind(), kvs::end_point_kind::exclusive);
            EXPECT_EQ(ranges[2]->begin().endpointkind(), kvs::end_point_kind::inclusive);
            EXPECT_EQ(ranges[2]->end().endpointkind(), kvs::end_point_kind::exclusive);
            EXPECT_EQ(ranges[3]->begin().endpointkind(), kvs::end_point_kind::inclusive);
            EXPECT_EQ(ranges[3]->end().endpointkind(), kvs::end_point_kind::prefixed_exclusive);
        });
}

TEST_F(scan_test, scan_info_rtx_parallel_enabled_by_transaction_context) {
    // issues #1196: enable rtx parallel scan via optional setting in transaction context
    do_rtx_range_test(
        0,
        std::make_shared<api::transaction_option>(
            transaction_type_kind::rtx,
            std::vector<std::string>{"table1", "table2"},
            "",
            std::vector<std::string>{"area1", "area2"},
            std::vector<std::string>{"area3", "area4"},
            false,
            10),  // parallelism set for this transaction
        [](auto const& ranges) {
            ASSERT_EQ(10, ranges.size());
        });
}

/**
 * @brief Scan with lower inclusive bound and upper unbound on a single int4 primary key.
 *
 * Rows (10,1),(20,2),(30,3),(40,4). Lower inclusive 20 returns rows with key >= 20.
 */
TEST_F(scan_test, endpoint_lower_prefixed_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_single_col_pk_table();
    insert_endpoint_test_rows(setup);
    auto result = run_single_col_pk_scan(
        setup,
        make_int4_endpoint(setup, 20, ek::prefixed_inclusive),
        {}
    );
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 2)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 3)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(40, 4)), result[2]);
}

/**
 * @brief Scan with lower prefixed_exclusive bound and upper unbound on a single int4 primary key.
 *
 * Rows (10,1),(20,2),(30,3),(40,4). Prefixed exclusive 20 skips all entries with the prefix key,
 * which for a fixed-size single column is equivalent to exclusive (key > 20).
 */
TEST_F(scan_test, endpoint_lower_prefixed_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_single_col_pk_table();
    insert_endpoint_test_rows(setup);
    auto result = run_single_col_pk_scan(
        setup,
        make_int4_endpoint(setup, 20, ek::prefixed_exclusive),
        {}
    );
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 3)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(40, 4)), result[1]);
}

/**
 * @brief Scan with lower unbound and upper inclusive bound on a single int4 primary key.
 *
 * Rows (10,1),(20,2),(30,3),(40,4). Upper inclusive 30 returns rows with key <= 30.
 */
TEST_F(scan_test, endpoint_upper_prefixed_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_single_col_pk_table();
    insert_endpoint_test_rows(setup);
    auto result = run_single_col_pk_scan(
        setup,
        {},
        make_int4_endpoint(setup, 30, ek::prefixed_inclusive)
    );
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 1)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 2)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 3)), result[2]);
}

/**
 * @brief Scan with lower unbound and upper prefixed_exclusive bound on a single int4 primary key.
 *
 * Rows (10,1),(20,2),(30,3),(40,4). Prefixed exclusive 30 stops before entries with the prefix key,
 * which for a fixed-size single column is equivalent to exclusive (key < 30).
 */
TEST_F(scan_test, endpoint_upper_prefixed_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_single_col_pk_table();
    insert_endpoint_test_rows(setup);
    auto result = run_single_col_pk_scan(
        setup,
        {},
        make_int4_endpoint(setup, 30, ek::prefixed_exclusive)
    );
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 1)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 2)), result[1]);
}

// ─── Composite PK (C0, C1) endpoint tests ──────────────────────────────────
//
// Valid lower/upper patterns per fix-scan-secondary-indices.md:
//   The number of columns in lower and upper may differ by at most 1.
//   Range search occurs only on the trailing column of the longer endpoint.
//
// For a 2-column composite PK (C0, C1), the valid (lower_cols, upper_cols)
// structural patterns are: (1,0), (0,1), (1,1), (1,2), (2,1), (2,2).
// (0,0) is the full scan, already covered by scan_test.simple.
// (0,2) and (2,0) are invalid (diff > 1) and are not tested here.
//
// Partial-key endpoint semantics on composite keys (byte-string ordering):
// A 4-byte partial key for C0 compares as strictly less than any 8-byte
// composite key sharing that C0 prefix.  As a result:
//   lower inclusive  ≈ lower exclusive  ≈ lower prefixed_inclusive
//     → all include the C0=value row  (8-byte key > 4-byte begin key)
//   lower prefixed_exclusive
//     → skips all rows with this C0 prefix
//   upper inclusive  ≈ upper exclusive  ≈ upper prefixed_exclusive
//     → all exclude the C0=value rows  (8-byte key > 4-byte end key)
//   upper prefixed_inclusive
//     → includes all rows with this C0 prefix
//
// ─── (lower=1col, upper=unbound) — range on C0 ─────────────────────────────
// Data: (C0=10,C1=10,C2=1),(C0=20,C1=10,C2=2),(C0=30,C1=10,C2=3),(C0=40,C1=10,C2=4)

/**
 * @brief Composite PK lower=1col inclusive, upper=unbound.
 *
 * For a partial (C0-only) lower bound on a (C0,C1) composite key, `inclusive`
 * starts from entries whose key >= 4-byte encoded(C0).  All composite entries
 * with C0=20 have 8-byte keys that exceed the 4-byte key, so they are
 * included.  Returns rows with C0 >= 20.
 */
TEST_F(scan_test, composite_pk_lower_prefixed_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_pk_table();
    insert_composite_pk_c0_rows(setup);
    auto result = run_composite_pk_scan(
        setup, make_int4_endpoint(setup, 20, ek::prefixed_inclusive), {});
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(20, 10, 2)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(30, 10, 3)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(40, 10, 4)), result[2]);
}

/**
 * @brief Composite PK lower=1col prefixed_exclusive, upper=unbound.
 *
 * `prefixed_exclusive` skips all entries whose key starts with the prefix
 * encoded(C0=20), resuming from the first entry with C0 > 20.
 * Returns rows with C0 > 20.
 */
TEST_F(scan_test, composite_pk_lower_prefixed_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_pk_table();
    insert_composite_pk_c0_rows(setup);
    auto result = run_composite_pk_scan(
        setup, make_int4_endpoint(setup, 20, ek::prefixed_exclusive), {});
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(30, 10, 3)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(40, 10, 4)), result[1]);
}

// ─── (lower=unbound, upper=1col) — range on C0 ─────────────────────────────

/**
 * @brief Composite PK lower=unbound, upper=1col inclusive.
 *
 * For a partial (C0-only) upper bound, `inclusive` stops at entries whose key
 * <= 4-byte encoded(C0).  All composite entries with C0=30 have 8-byte keys
 * that exceed the 4-byte end key, so they are excluded.
 * Returns rows with C0 < 30.
 */
TEST_F(scan_test, composite_pk_upper_prefixed_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_pk_table();
    insert_composite_pk_c0_rows(setup);
    auto result = run_composite_pk_scan(
        setup, {}, make_int4_endpoint(setup, 30, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 10, 1)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(20, 10, 2)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(30, 10, 3)), result[2]);
}

/**
 * @brief Composite PK lower=unbound, upper=1col prefixed_exclusive.
 *
 * `prefixed_exclusive` stops before entries whose key starts with the prefix
 * encoded(C0=30), so C0=30 rows are excluded — same result as inclusive/exclusive
 * for partial upper keys.
 * Returns rows with C0 < 30.
 */
TEST_F(scan_test, composite_pk_upper_prefixed_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_pk_table();
    insert_composite_pk_c0_rows(setup);
    auto result = run_composite_pk_scan(
        setup, {}, make_int4_endpoint(setup, 30, ek::prefixed_exclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 10, 1)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(20, 10, 2)), result[1]);
}

// ─── (lower=1col, upper=1col) — range on C0 ────────────────────────────────

/**
 * @brief Composite PK lower=1col inclusive, upper=1col exclusive.
 *
 * lower inclusive on C0=20: includes all C0=20 rows (8-byte key > 4-byte key).
 * upper exclusive on C0=30: stops before 4-byte key, excluding C0=30 rows.
 * Returns C0=20 row only.
 */
TEST_F(scan_test, composite_pk_lower1_upper1_prefixed_exclusive_prefixed_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_pk_table();
    insert_composite_pk_c0_rows(setup);
    auto result = run_composite_pk_scan(
        setup,
        make_int4_endpoint(setup, 20, ek::prefixed_exclusive),
        make_int4_endpoint(setup, 30, ek::prefixed_inclusive));
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(30, 10, 3)), result[0]);
}

/**
 * @brief Composite PK lower=1col inclusive, upper=1col prefixed_inclusive.
 *
 * lower inclusive on C0=20: includes C0=20 and above.
 * upper prefixed_inclusive on C0=30: includes all C0=30 rows.
 * Returns C0=20 and C0=30 rows.
 */
TEST_F(scan_test, composite_pk_lower2col_upper1col_prefixed_inclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_pk_table();
    insert_composite_pk_c1_rows(setup);
    auto result = run_composite_pk_scan(
        setup,
        make_int4_2col_endpoint(setup, 10, 20, ek::prefixed_inclusive),
        make_int4_endpoint(setup, 10, ek::prefixed_inclusive));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 20, 2)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 30, 3)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 40, 4)), result[2]);
}

/**
 * @brief Composite PK lower=2col prefixed_exclusive, upper=1col(C0) prefixed_inclusive.
 *
 * lower is (C0=10, C1=20, prefixed_exclusive); upper C1=+INF.
 * For fixed-size int4, prefixed_exclusive equals exclusive on a full key.
 * Returns rows with C0=10 and C1 > 20.
 */
TEST_F(scan_test, composite_pk_lower2col_upper1col_prefixed_exclusive) {
    using ek = relation::endpoint_kind;
    auto setup = prepare_composite_pk_table();
    insert_composite_pk_c1_rows(setup);
    auto result = run_composite_pk_scan(
        setup,
        make_int4_2col_endpoint(setup, 10, 20, ek::prefixed_exclusive),
        make_int4_endpoint(setup, 10, ek::prefixed_inclusive));
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 30, 3)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 40, 4)), result[1]);
}

/**
 * @brief Full scan with both endpoints explicitly passed as unbound.
 *
 * Equivalent to passing no endpoints at all; all rows including null C1 values
 * are returned.
 */
TEST_F(scan_test, full_scan_explicit_unbound) {
    auto setup = prepare_indices(
        {"T0", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{true}},
        }},
        {0}, {}
    );
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, std::nullopt), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 300), *db_);

    auto& target = add_scan_node(setup, false, {}, {});
    auto out = create_nullable_record<kind::int4, kind::int4>();
    auto down = add_downstream_record_verifier(destinations(target.columns()));
    auto tx = wrap(db_->create_transaction());
    auto ex = make_scan_executor(target, setup, false, down, out, tx);
    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });
    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, std::nullopt)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 300)), result[2]);
    ASSERT_EQ(status::ok, tx->commit());
}

/**
 * @brief Scan with lower > upper (reversed bounds) returns empty result.
 */
TEST_F(scan_test, reversed_bounds) {
    namespace tv = takatori::value;
    namespace tt = takatori::type;
    using ek = relation::endpoint_kind;

    auto setup = prepare_indices(
        {"T0", {
            {"C0", t::int4(), nullity{false}},
            {"C1", t::int4(), nullity{false}},
        }},
        {0}, {}
    );
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(10, 100), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(20, 200), *db_);
    put_row(setup, create_nullable_record<kind::int4, kind::int4>(30, 300), *db_);

    auto& target = add_scan_node(
        setup, false,
        make_scan_endpoint(
            setup, {0}, make_exprs(std::make_unique<scalar::immediate>(tv::int4(30), tt::int4())), ek::prefixed_inclusive
        ),
        make_scan_endpoint(
            setup, {0}, make_exprs(std::make_unique<scalar::immediate>(tv::int4(10), tt::int4())), ek::prefixed_inclusive
        )
    );
    auto out = create_nullable_record<kind::int4, kind::int4>();
    auto down = add_downstream_record_verifier(destinations(target.columns()));
    auto tx = wrap(db_->create_transaction());
    auto ex = make_scan_executor(target, setup, false, down, out, tx);
    std::vector<basic_record> result{};
    down.set_body([&]() {
        result.emplace_back(get_variables(ex.variables_list_[0], destinations(target.columns())));
    });
    ASSERT_TRUE(static_cast<bool>(ex.op_(ex.ctx_)));
    ex.ctx_.release();
    ASSERT_EQ(0, result.size());
    ASSERT_EQ(status::ok, tx->commit());
}

}  // namespace jogasaki::executor::process::impl::ops
