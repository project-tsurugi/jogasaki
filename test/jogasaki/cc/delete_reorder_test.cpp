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
using jogasaki::mock::create_nullable_record;
using kind = meta::field_type_kind;

class delete_reorder_test :
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
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

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

TEST_F(delete_reorder_test, delete_forwarded_before_insert) {
    // low priority tx1 (delete) is forwarded before high priority tx0 (insert)
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY, C1 INT)");
    {
        auto tx0 = utils::create_transaction(*db_, false, true, {"T"});
        wait_epochs(1);
        auto tx1 = utils::create_transaction(*db_, false, true, {"T"});
        execute_statement("INSERT INTO T (C0, C1) VALUES (2, 2)", *tx0);  // w of rw
        execute_statement("DELETE FROM T WHERE C0=2", *tx1);  // r of rw
        ASSERT_EQ(status::ok, tx0->commit());
        ASSERT_EQ(status::ok, tx1->commit());
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T WHERE C0=2", result);
            EXPECT_EQ(1, result.size());
        }
    }
}

// TODO need investigation after fix
TEST_F(delete_reorder_test, DISABLED_delete_forwarded_before_insert_existing_rec) {
    // similar to delete_forwarded_before_insert, but there is existing rec
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T (C0, C1) VALUES (2, 2)");
    {
        auto tx0 = utils::create_transaction(*db_, false, true, {"T"});
        wait_epochs(1);
        auto tx1 = utils::create_transaction(*db_, false, true, {"T"});
        execute_statement("INSERT OR REPLACE INTO T (C0, C1) VALUES (2, 20)", *tx0);  // w of rw
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T WHERE C0=2", *tx1, result);
            ASSERT_EQ(1, result.size());
            EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2,2)), result[0]);
        }
        execute_statement("DELETE FROM T WHERE C0=2", *tx1);  // r of rw
        ASSERT_EQ(status::ok, tx0->commit());
        ASSERT_EQ(status::ok, tx1->commit());
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T WHERE C0=2", result);
            ASSERT_EQ(1, result.size());
            EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2,20)), result[0]);
        }
    }
}

TEST_F(delete_reorder_test, DISABLED_insert_forwarded_before_delete) {
    // low priority tx1 (delete) is forwarded before high priority tx0 (insert)
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY, C1 INT)");
    {
        auto tx0 = utils::create_transaction(*db_, false, true, {"T"});
        wait_epochs(1);
        auto tx1 = utils::create_transaction(*db_, false, true, {"T"});
        execute_statement("INSERT OR REPLACE INTO T VALUES (1,1)", *tx0);  // w of rw
        execute_statement("DELETE FROM T WHERE C0=2", *tx0);
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T WHERE C1=1", *tx1, result);  // r of rw
            EXPECT_EQ(0, result.size());
        }
        execute_statement("INSERT OR REPLACE INTO T (C0, C1) VALUES (2, 2)", *tx1);
        ASSERT_EQ(status::ok, tx0->commit());
        ASSERT_EQ(status::ok, tx1->commit());
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T WHERE C0=2", result);
            EXPECT_EQ(0, result.size());
        }
    }
}

}
