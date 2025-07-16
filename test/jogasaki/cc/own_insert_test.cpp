/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <functional>
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
using namespace jogasaki::mock;

using takatori::util::unsafe_downcast;

class own_insert_test :
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

static void run(
    api::database& db,
    std::function<void()> pre,
    std::function<void(api::transaction_handle)> body,
    std::function<void()> post
) {
    {
        SCOPED_TRACE("test occ transaction");
        pre();
        {
            auto tx = utils::create_transaction(db, false, false, {});
            body(*tx);
        }
        post();
    }
    {
        SCOPED_TRACE("test long transaction");
        pre();
        {
            auto tx = utils::create_transaction(db, true);
            body(*tx);
        }
        post();
    }
}

TEST_F(own_insert_test, select_can_see_own_insert) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", tx0);
            {
                // point query can see insert
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2", tx0, result);
                ASSERT_EQ(1, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(2,2.0)), result[0]);
            }
            {
                // range query can see insert
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0", tx0, result);
                ASSERT_EQ(2, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,1.0)), result[0]);
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(2,2.0)), result[1]);
            }
            ASSERT_EQ(status::ok, tx0.commit());
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}

TEST_F(own_insert_test, point_delete_can_see_own_insert) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", tx0);
            // point delete inserted record successfully
            execute_statement("DELETE FROM T0 WHERE C0=2", tx0);
            ASSERT_EQ(status::ok, tx0.commit());
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2 ORDER BY C0", result);
                ASSERT_EQ(0, result.size());
            }
            {
                //verify with range scan
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 ORDER BY C0", result);
                ASSERT_EQ(1, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,1.0)), result[0]);
            }
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}

TEST_F(own_insert_test, range_delete_can_see_own_insert) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", tx0);
            // range delete inserted record successfully
            execute_statement("DELETE FROM T0", tx0);
            ASSERT_EQ(status::ok, tx0.commit());
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2 ORDER BY C0", result);
                EXPECT_EQ(0, result.size());
            }
            {
                //verify with range scan
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 ORDER BY C0", result);
                EXPECT_EQ(0, result.size());
            }
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}

TEST_F(own_insert_test, insert_can_see_own_insert) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", tx0);
            // range delete inserted record successfully
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", tx0, status::err_unique_constraint_violation);
            ASSERT_EQ(status::err_inactive_transaction, tx0.commit());
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}

TEST_F(own_insert_test, point_update_can_see_own_insert) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", tx0);
            // point update inserted record successfully
            execute_statement("UPDATE T0 SET C1=20.0 WHERE C0=2", tx0);
            ASSERT_EQ(status::ok, tx0.commit());
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2", result);
                ASSERT_EQ(1, result.size());
//                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(2,20.0)), result[0]);
            }
            {
                //verify with range query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 ORDER BY C0", result);
                ASSERT_EQ(2, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,1.0)), result[0]);
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(2,20.0)), result[1]);
            }
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}

TEST_F(own_insert_test, range_update_can_see_own_insert) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", tx0);
            // point update inserted record successfully
            execute_statement("UPDATE T0 SET C1=20.0", tx0);
            ASSERT_EQ(status::ok, tx0.commit());
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2", result);
                ASSERT_EQ(1, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(2,20.0)), result[0]);
            }
            {
                //verify with range query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 ORDER BY C0", result);
                ASSERT_EQ(2, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,20.0)), result[0]);
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(2,20.0)), result[1]);
            }
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}

TEST_F(own_insert_test, point_pk_update_can_see_own_insert) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", tx0);
            // point update inserted record successfully
            execute_statement("UPDATE T0 SET C0=20 WHERE C0=2", tx0);
            ASSERT_EQ(status::ok, tx0.commit());
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=20", result);
                ASSERT_EQ(1, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(20,2.0)), result[0]);
            }
            {
                //verify with range query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 ORDER BY C0", result);
                ASSERT_EQ(2, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,1.0)), result[0]);
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(20,2.0)), result[1]);
            }
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}

// TODO range pk update is not implemented yet, add testcases when it's available

}
