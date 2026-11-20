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
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace jogasaki::mock;
using namespace std::string_view_literals;

/**
 * @brief tests for OCC statements which scan the table and insert the new keys into the same table
 * @details the statement reads the rows by the scan (with range or OR condition) and inserts the new keys (e.g. UPDATE
 * changing the primary key), so the inserts can change the index nodes read by the scan of the same transaction.
 * Such inserts by the transaction itself must not be regarded as phantom (tsurugi-issues#1589).
 */
class sql_occ_write_after_scan_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        cfg_ = std::make_shared<configuration>();
        db_setup(cfg_);
        utils::set_global_tx_option(utils::create_tx_option{false, true});  // force OCC
        execute_statement("CREATE TABLE t (c1 INT NOT NULL PRIMARY KEY, v VARCHAR(4))");
        execute_statement("INSERT INTO t VALUES (1000, 'a'), (200, 'b'), (10, 'c')");
    }

    void TearDown() override {
        utils::set_global_tx_option(utils::create_tx_option{});
        db_teardown();
    }

    std::vector<std::int32_t> keys() {
        std::vector<basic_record> result{};
        execute_query("SELECT c1 FROM t", result);
        std::vector<std::int32_t> ret{};
        ret.reserve(result.size());
        for(auto&& rec : result) {
            ret.emplace_back(rec.get_value<std::int32_t>(0));
        }
        std::sort(ret.begin(), ret.end());
        return ret;
    }

    std::shared_ptr<configuration> cfg_{};  //NOLINT
};

TEST_F(sql_occ_write_after_scan_test, update_primary_key_with_or_condition) {
    execute_statement("UPDATE t SET c1 = c1 + 10 WHERE c1 = 10 OR c1 = 200");
    EXPECT_EQ((std::vector<std::int32_t>{20, 210, 1000}), keys());
}

TEST_F(sql_occ_write_after_scan_test, update_primary_key_with_range) {
    execute_statement("UPDATE t SET c1 = c1 + 10 WHERE c1 >= 10 AND c1 <= 300");
    EXPECT_EQ((std::vector<std::int32_t>{20, 210, 1000}), keys());
}

TEST_F(sql_occ_write_after_scan_test, update_primary_key_out_of_range) {
    // the new keys are out of the scan range
    execute_statement("UPDATE t SET c1 = c1 + 5000 WHERE c1 >= 10 AND c1 <= 200");
    EXPECT_EQ((std::vector<std::int32_t>{1000, 5010, 5200}), keys());
}

TEST_F(sql_occ_write_after_scan_test, update_primary_key_without_condition) {
    execute_statement("UPDATE t SET c1 = c1 + 1");
    EXPECT_EQ((std::vector<std::int32_t>{11, 201, 1001}), keys());
}

TEST_F(sql_occ_write_after_scan_test, insert_select_with_range) {
    execute_statement("INSERT INTO t SELECT c1 + 1, v FROM t WHERE c1 >= 10 AND c1 <= 300");
    EXPECT_EQ((std::vector<std::int32_t>{10, 11, 200, 201, 1000}), keys());
}

}  // namespace jogasaki::testing
