/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <future>
#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/utils/runner.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/utils/tables.h>

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
        utils::add_test_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
        execute_statement("create table qa_t1 (c_pk int primary key, c_i4 int not null, c_i8 bigint not null, c_f4 real not null, c_f8 double not null, c_ch varchar(*) not null)");
        execute_statement("create index qa_t1_i4_idx on qa_t1(c_i4)");
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

class block_verifier {
public:

    auto exec(std::function<void(void)> f, std::size_t wait_ns = 10*1000*1000) {
        using namespace std::chrono_literals;
        std::atomic_bool finished{false};
        auto g = std::async(std::launch::async, [=](){
            f();
            finished_ = true;
        });
        g.wait_for(std::chrono::nanoseconds{wait_ns});
        return g;
    }

    bool finished() {
        return finished_;
    }
private:
    std::atomic_bool finished_{false};
};


TEST_F(long_tx_test, long_insert_long_insert2) {
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx1);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx2);
    block_verifier vf{};
    auto f = vf.exec([&](){ tx2->commit(); });
    ASSERT_FALSE(vf.finished());
    ASSERT_EQ(status::ok, tx1->commit());
    f.get();
    ASSERT_TRUE(vf.finished());
}

TEST_F(long_tx_test, long_insert_long_insert3) {
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx2);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx1);
    block_verifier vf{};
    auto f = vf.exec([&](){ tx2->commit(); });
    ASSERT_FALSE(vf.finished());
    ASSERT_EQ(status::ok, tx1->commit());
    f.get();
    ASSERT_TRUE(vf.finished());
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

TEST_F(long_tx_test, multiple_tx_iud_same_key) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("UPDATE T0 SET C1=10.0 WHERE C0=1", *tx1);
    execute_statement("UPDATE T0 SET C1=20.0 WHERE C0=1", *tx2);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::err_serialization_failure, tx2->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 10.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[1]);
}

TEST_F(long_tx_test, multiple_tx_iud_different_key) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("UPDATE T0 SET C1=10.0 WHERE C0=1", *tx1);
    execute_statement("UPDATE T0 SET C1=20.0 WHERE C0=2", *tx2);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::ok, tx2->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 10.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 20.0)), result[1]);
}

TEST_F(long_tx_test, reading_others_wp_prep_by_ltx) {
    auto tx = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx);
    ASSERT_EQ(status::ok, tx->commit());
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {});
    execute_statement("SELECT * FROM T0 WHERE C0=2", *tx2);
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
    execute_statement("SELECT * FROM T0 WHERE C0=2", *tx2);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::ok, tx2->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 1.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[1]);
}

TEST_F(long_tx_test, reading_others_wp_after_commit) {
    // TODO currently working fine.
    {
        auto tx = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx);
        execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx);
        ASSERT_EQ(status::ok, tx->commit());
    }
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("UPDATE T0 SET C1=C1+10.0 WHERE C0=1", *tx1);
    ASSERT_EQ(status::ok, tx1->commit());

    execute_statement("UPDATE T0 SET C1=C1+10.0 WHERE C0=1", *tx2);
    ASSERT_EQ(status::ok, tx2->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 21.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[1]);
}

TEST_F(long_tx_test, update_delete_ltx) {
    {
        auto tx = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx);
        execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx);
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("UPDATE T0 SET C1=10.0 WHERE C0=1", *tx1);
        ASSERT_EQ(status::ok, tx1->commit());
    }
    {
        auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("UPDATE T0 SET C1=100.0 WHERE C0=1", *tx2);
        ASSERT_EQ(status::ok, tx2->commit());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 ORDER BY C0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 100.0)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[1]);
    }
    {
        auto tx3 = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("DELETE FROM T0 WHERE C0=1", *tx3);
        ASSERT_EQ(status::ok, tx3->commit());
    }
    {
        wait_epochs(5);
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 WHERE C0=1", result);
        ASSERT_EQ(0, result.size());
        execute_query("SELECT * FROM T0 WHERE C0=2", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[0]);
    }
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
}

TEST_F(long_tx_test, update_primary_key) {
    {
        auto tx = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx);
        execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx);
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("UPDATE T0 SET C0=10 WHERE C0=1", *tx1);
        ASSERT_EQ(status::ok, tx1->commit());
    }
    {
        auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("UPDATE T0 SET C0=100 WHERE C0=10", *tx2);
        ASSERT_EQ(status::ok, tx2->commit());
    }
    wait_epochs(2); // TODO update contains delete op. shirakami delete delays to be visible
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 WHERE C0=100", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(100, 1.0)), result[0]);
        result.clear();
        execute_query("SELECT * FROM T0 WHERE C0=2", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[0]);
    }
    {
        auto tx3 = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("DELETE FROM T0 WHERE C0=100", *tx3);
        ASSERT_EQ(status::ok, tx3->commit());
    }
    {
        wait_epochs(2); // TODO shirakami delete delays to be visible
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 WHERE C0=100", result);
        ASSERT_EQ(0, result.size());
        execute_query("SELECT * FROM T0 WHERE C0=2", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[0]);
    }
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (100, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
}

TEST_F(long_tx_test, update_secondary_key) {
    {
        auto tx = utils::create_transaction(*db_, false, true, {"TSECONDARY"});
        execute_statement("INSERT INTO TSECONDARY (C0, C1) VALUES (1, 1)", *tx);
        execute_statement("INSERT INTO TSECONDARY (C0, C1) VALUES (2, 2)", *tx);
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        auto tx1 = utils::create_transaction(*db_, false, true, {"TSECONDARY"});
        execute_statement("UPDATE TSECONDARY SET C1=10 WHERE C1=1", *tx1);
        ASSERT_EQ(status::ok, tx1->commit());
    }
    {
        auto tx2 = utils::create_transaction(*db_, false, true, {"TSECONDARY"});
        execute_statement("UPDATE TSECONDARY SET C1=100 WHERE C1=10", *tx2);
        ASSERT_EQ(status::ok, tx2->commit());
    }
    wait_epochs(2); // TODO update contains delete op. shirakami delete delays to be visible
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TSECONDARY WHERE C1=100", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::int8>(1, 100)), result[0]);
        result.clear();
        execute_query("SELECT * FROM TSECONDARY WHERE C1=2", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::int8>(2, 2)), result[0]);
    }
    {
        auto tx3 = utils::create_transaction(*db_, false, true, {"TSECONDARY"});
        execute_statement("DELETE FROM TSECONDARY WHERE C1=100", *tx3);
        ASSERT_EQ(status::ok, tx3->commit());
    }
    {
        wait_epochs(2); // TODO shirakami delete delays to be visible
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TSECONDARY WHERE C1=100", result);
        ASSERT_EQ(0, result.size());
        execute_query("SELECT * FROM TSECONDARY WHERE C1=2", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::int8>(2, 2.0)), result[0]);
    }
    execute_statement("INSERT INTO TSECONDARY (C0, C1) VALUES (1, 100)");
}

TEST_F(long_tx_test, scan) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (3, 3.0)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {});
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", *tx1, result);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::ok, tx2->commit());
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 1.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(3, 3.0)), result[2]);
}

TEST_F(long_tx_test, scan_and_delete) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (3, 3.0)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("DELETE FROM T0 WHERE C0=2", *tx1);
    ASSERT_EQ(status::ok, tx1->commit());
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", *tx2, result);
    ASSERT_EQ(status::ok, tx2->commit());
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 1.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(3, 3.0)), result[1]);
}

TEST_F(long_tx_test, scan_and_delete2) {
    // concurrent delete sometimes cauase zero length key
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (3, 3.0)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("DELETE FROM T0 WHERE C0=2", *tx1);
    ASSERT_EQ(status::ok, tx1->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", *tx2, result);
    ASSERT_EQ(status::ok, tx2->commit());
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 1.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(3, 3.0)), result[1]);
}

TEST_F(long_tx_test, commit_wait) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (3, 3.0)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx2);

    block_verifier vf{};
    auto f = vf.exec([&](){ tx2->commit(); });
    ASSERT_FALSE(vf.finished());
    ASSERT_EQ(status::ok, tx1->commit());
    f.get();
    ASSERT_TRUE(vf.finished());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 1.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(3, 3.0)), result[2]);
}

TEST_F(long_tx_test, commit_wait_error) {
    // wait and abort
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (3, 3.0)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx2);

    block_verifier vf{};
    status st2{};
    auto f = vf.exec([&](){ st2 = tx2->commit(); }, 1000*1000*1000); // default wait is too short to observe commit wait
    ASSERT_FALSE(vf.finished());
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx1);
    ASSERT_EQ(status::ok, tx1->commit());
    f.get();
    EXPECT_EQ(status::err_serialization_failure, st2);
    ASSERT_TRUE(vf.finished());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 1.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 2.0)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(3, 3.0)), result[2]);
}

TEST_F(long_tx_test, start_wait) {
    block_verifier vf{};
    std::shared_ptr<api::transaction_handle> tx1{};
    auto f = vf.exec([&](){
        //debug log shows WAITING_START messages
        tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    }, 1000*1000); // = 1ms. Waiting epoch takes 10-40 ms, so we expect this will not complete in 1ms.
    ASSERT_FALSE(vf.finished());
    wait_epochs(1);
    f.get();
    ASSERT_TRUE(vf.finished());
    ASSERT_EQ(status::ok, tx1->commit());
}

TEST_F(long_tx_test, occ_accessing_wp) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    {
        auto tx2 = utils::create_transaction(*db_, false, false);
        test_stmt_err("SELECT * FROM T0 WHERE C0=1", *tx2, error_code::cc_exception);
        ASSERT_EQ(status::ok, tx2->abort());
    }
    {
        auto tx2 = utils::create_transaction(*db_, false, false);
        test_stmt_err("DELETE FROM T0 WHERE C0=1", *tx2, error_code::cc_exception);
        ASSERT_EQ(status::ok, tx2->abort());
    }
    {
        auto tx2 = utils::create_transaction(*db_, false, false);
        test_stmt_err("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx2, error_code::conflict_on_write_preserve_exception);
        ASSERT_EQ(status::ok, tx2->abort());
    }
    {
        auto tx2 = utils::create_transaction(*db_, false, false);
        test_stmt_err("UPDATE T0 SET C1=3.0 WHERE C1=1", *tx2, error_code::cc_exception);
        ASSERT_EQ(status::ok, tx2->abort());
    }
}
}
