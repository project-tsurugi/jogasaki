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
#include <jogasaki/utils/add_test_tables.h>
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

class own_delete_test :
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
        utils::add_test_tables();
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

TEST_F(own_delete_test, select_not_see_own_point_deleted) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("DELETE FROM T0 WHERE C0=2", tx0);
            {
                // point query not see deleted
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2", tx0, result);
                ASSERT_EQ(0, result.size());
            }
            {
                // range query not see deleted
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 ORDER BY C0", tx0, result);
                ASSERT_EQ(1, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,1.0)), result[0]);
            }
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}

TEST_F(own_delete_test, select_not_see_own_range_deleted) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("DELETE FROM T0", tx0);
            {
                // point query not see deleted
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2", tx0, result);
                ASSERT_EQ(0, result.size());
            }
            {
                // range query not see deleted
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 ORDER BY C0", tx0, result);
                ASSERT_EQ(0, result.size());
            }
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}

// TODO scenarios where delete cannot see deleted recrods are not implemented yet because delete doesn't return error
// for empty deletion. Implement tests when delete returns the number of records in the future.

TEST_F(own_delete_test, insert_not_see_own_point_deleted) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("DELETE FROM T0 WHERE C0=2", tx0);
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", tx0);
            ASSERT_EQ(status::ok, tx0.commit());
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2", result);
                ASSERT_EQ(1, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(2,2.0)), result[0]);
            }
            {
                //verify with range query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 ORDER BY C0", result);
                ASSERT_EQ(2, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,1.0)), result[0]);
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(2,2.0)), result[1]);
            }
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}

TEST_F(own_delete_test, insert_not_see_own_range_deleted) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("DELETE FROM T0", tx0);
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", tx0);
            ASSERT_EQ(status::ok, tx0.commit());
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2", result);
                ASSERT_EQ(1, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(2,2.0)), result[0]);
            }
            {
                //verify with range query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 ORDER BY C0", result);
                ASSERT_EQ(1, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(2,2.0)), result[0]);
            }
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}

TEST_F(own_delete_test, point_update_not_see_point_deleted) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("DELETE FROM T0 WHERE C0=2", tx0);
            execute_statement("UPDATE T0 SET C1=20.0 WHERE C0=2", tx0); // nothing to update and no error
            ASSERT_EQ(status::ok, tx0.commit());
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2", result);
                ASSERT_EQ(0, result.size());
            }
            {
                //verify with range query
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

TEST_F(own_delete_test, range_update_not_see_point_deleted) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("DELETE FROM T0 WHERE C0=2", tx0);
            execute_statement("UPDATE T0 SET C1=20.0", tx0); // update only C0=1
            ASSERT_EQ(status::ok, tx0.commit());
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2", result);
                ASSERT_EQ(0, result.size());
            }
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=1", result);
                ASSERT_EQ(1, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,20.0)), result[0]);
            }
            {
                //verify with range query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 ORDER BY C0", result);
                ASSERT_EQ(1, result.size());
                EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,20.0)), result[0]);
            }
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}


TEST_F(own_delete_test, point_update_not_see_range_deleted) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("DELETE FROM T0", tx0);
            execute_statement("UPDATE T0 SET C1=20.0 WHERE C0=2", tx0); // nothing to update
            ASSERT_EQ(status::ok, tx0.commit());
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2", result);
                ASSERT_EQ(0, result.size());
            }
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=1", result);
                ASSERT_EQ(0, result.size());
            }
            {
                //verify with range query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 ORDER BY C0", result);
                ASSERT_EQ(0, result.size());
            }
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}

TEST_F(own_delete_test, range_update_not_see_range_deleted) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("DELETE FROM T0", tx0);
            execute_statement("UPDATE T0 SET C1=20.0", tx0); // nothing to update
            ASSERT_EQ(status::ok, tx0.commit());
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2", result);
                ASSERT_EQ(0, result.size());
            }
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=1", result);
                ASSERT_EQ(0, result.size());
            }
            {
                //verify with range query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 ORDER BY C0", result);
                ASSERT_EQ(0, result.size());
            }
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}

TEST_F(own_delete_test, point_pk_update_not_see_own_point_delete) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("DELETE FROM T0 WHERE C0=2", tx0);
            execute_statement("UPDATE T0 SET C0=20 WHERE C0=2", tx0); // nothing to update
            ASSERT_EQ(status::ok, tx0.commit());
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2", result);
                ASSERT_EQ(0, result.size());
            }
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=20", result);
                ASSERT_EQ(0, result.size());
            }
            {
                //verify with range query
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

TEST_F(own_delete_test, point_pk_update_not_see_own_range_delete) {
    run(*db_,
        [&]() {
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
            execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
        },
        [&](api::transaction_handle tx0) {
            execute_statement("DELETE FROM T0", tx0);
            execute_statement("UPDATE T0 SET C0=20 WHERE C0=2", tx0); // nothing to update
            ASSERT_EQ(status::ok, tx0.commit());
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=2", result);
                ASSERT_EQ(0, result.size());
            }
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=20", result);
                ASSERT_EQ(0, result.size());
            }
            {
                //verify with point query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 WHERE C0=1", result);
                ASSERT_EQ(0, result.size());
            }
            {
                //verify with range query
                std::vector<mock::basic_record> result{};
                execute_query("SELECT * FROM T0 ORDER BY C0", result);
                ASSERT_EQ(0, result.size());
            }
        },
        [&]() {
            execute_statement("DELETE FROM T0");
        }
    );
}
// TODO range pk update is not implemented yet, add testcases when it's available
}
