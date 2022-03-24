/*
 * Copyright 2018-2020 tsurugi project.
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

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "../api/api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class long_tx_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
        auto* impl = db_impl();
        add_benchmark_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(long_tx_test, long_insert_long_insert0) {
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx1);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx2);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::ok, tx2->commit());
}

TEST_F(long_tx_test, long_insert_long_insert1) {
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx2);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx1);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::ok, tx2->commit());
}

TEST_F(long_tx_test, long_insert_long_insert2) {
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx1);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx2);
    ASSERT_EQ(status::err_aborted, tx2->commit()); // WP1 waits tx with higher priority TODO
    ASSERT_EQ(status::ok, tx1->commit());
}

TEST_F(long_tx_test, long_insert_long_insert3) {
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx2);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx1);
    ASSERT_EQ(status::err_aborted, tx2->commit()); // WP1 waits tx with higher priority TODO
    ASSERT_EQ(status::ok, tx1->commit());
}

TEST_F(long_tx_test, short_update) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("UPDATE T0 SET C1=2.0 WHERE C0=1");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto exp0 = mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 10.0);
    auto exp1 = mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 20.0);
    auto exp2 = mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(3, 30.0);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 2.0)), result[0]);
}

TEST_F(long_tx_test, long_update) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("UPDATE T0 SET C1=2.0 WHERE C0=1", *tx1);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (3, 3.0)", *tx1);
    ASSERT_EQ(status::ok, tx1->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 WHERE C0=1", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 2.0)), result[0]);
}

TEST_F(long_tx_test, multiple_tx_insert) {
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx1);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx2);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::ok, tx2->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 1.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[1]);
}

TEST_F(long_tx_test, multiple_tx_insert2) {
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx1);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx2);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (3, 3.0)", *tx1);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (4, 4.0)", *tx2);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::ok, tx2->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(4, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 1.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(3, 3.0)), result[2]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(4, 4.0)), result[3]);
}

TEST_F(long_tx_test, multiple_tx_iud) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("UPDATE T0 SET C1=10.0 WHERE C0=1", *tx1);
    execute_statement("UPDATE T0 SET C1=20.0 WHERE C0=2", *tx2, status::err_aborted); // WP-1 raises ERR_FAIL_WP when reading into WP whose tx is not yet committed TODO
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::ok, tx2->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 10.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[1]);
}

TEST_F(long_tx_test, reading_others_wp_prep_by_ltx) {
    auto tx = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx);
    ASSERT_EQ(status::ok, tx->commit());
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {});
    execute_statement("SELECT * FROM T0 WHERE C0=2", *tx2, status::err_aborted); // WP-0+alpha raises ERR_FAIL_WP when reading into WP whose tx is not yet committed TODO
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::ok, tx2->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 1.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[1]);
}

TEST_F(long_tx_test, reading_others_wp_prep_by_stx) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {});
    execute_statement("SELECT * FROM T0 WHERE C0=2", *tx2, status::err_aborted); // WP-0+alpha raises ERR_FAIL_WP when reading into WP whose tx is not yet committed TODO
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::ok, tx2->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 1.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[1]);
}

TEST_F(long_tx_test, reading_others_wp_after_commit) {
    {
        auto tx = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx);
        execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx);
        ASSERT_EQ(status::ok, tx->commit());
    }
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("UPDATE T0 SET C1=10.0 WHERE C0=1", *tx1);
    ASSERT_EQ(status::ok, tx1->commit());

    execute_statement("UPDATE T0 SET C1=100.0 WHERE C0=1", *tx2);
    ASSERT_EQ(status::ok, tx2->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 10.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[1]);
}
}
