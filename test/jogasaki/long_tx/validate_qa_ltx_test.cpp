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
#include <takatori/type/int.h>

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

class validate_qa_ltx_test :
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
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(validate_qa_ltx_test, crash_on_wp_build) {
    // once this scenario crashed with BUILD_WP=ON
    {
//        execute_statement("delete from qa_t1 where c_pk=1");
        execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')");
        std::vector<mock::basic_record> result{};
        execute_query("select c_pk from qa_t1 where c_pk=1", result);
        EXPECT_EQ(1, result.size());
    }
    {
        execute_statement("delete from qa_t1 where c_pk=1");
//        wait_epochs(40);
        execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')");
        std::vector<mock::basic_record> result{};
        execute_query("select c_pk from qa_t1 where c_pk=1", result);
        EXPECT_EQ(1, result.size());
    }
}

TEST_F(validate_qa_ltx_test, long_update) {
    // insertint to same page. This scenario once blocked and waited forever.
    execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')");
    auto tx = utils::create_transaction(*db_, false, true, {"qa_t1"});
    execute_statement("update qa_t1 set c_i4 = 3 where c_pk = 1", *tx);
    ASSERT_EQ(status::ok, tx->commit());
    std::vector<mock::basic_record> result{};
    execute_query("select c_i4 from qa_t1 where c_pk=1", result);
    EXPECT_EQ(1, result.size());
}

TEST_F(validate_qa_ltx_test, reading_others_wp) {
    execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')");
    execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (2, 20, 200, 2000.0, 20000.0, '200000')");
    auto tx1 = utils::create_transaction(*db_, false, true, {"qa_t1"});
    auto tx2 = utils::create_transaction(*db_, false, true, {});
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c_i8, c_f8 FROM qa_t1 ORDER BY c_pk", *tx2, result);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::ok, tx2->commit());
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(100, 10000.0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(200, 20000.0)), result[1]);
}
}
