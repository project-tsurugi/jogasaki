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

#include <cstdint>
#include <memory>
#include <sstream>
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
 * @brief tests for index_feature::ties_ordered_by_primary_key of the secondary indices (tsurugi-issues#1589)
 * @details the same tables are created with and without the feature (the feature is given when the index is
 * created), and the ordered query results are compared. The sort keys are unique (with the primary key), so the
 * results are deterministic. The parameter is use_rtx.
 */
class sql_ties_ordered_by_primary_key_test :
    public ::testing::TestWithParam<bool>,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        cfg_ = std::make_shared<configuration>();
        // enable rtx parallel scan to verify the ordered scan is not split
        cfg_->scan_default_parallel(4);
        db_setup(cfg_);
        for(bool ties : {true, false}) {
            cfg_->enable_ties_ordered_by_primary_key(ties);
            create_tables(ties ? "_on" : "_off");
        }
        cfg_->enable_ties_ordered_by_primary_key(true);
    }

    void TearDown() override {
        db_teardown();
    }

    // ev: dev has many ties and NULLs
    // ed: same as ev, but the primary key is descending
    // ec: the primary key is composite
    void create_tables(std::string_view suffix) {
        std::string s{suffix};
        execute_statement("CREATE TABLE ev" + s + " (id INT PRIMARY KEY, dev INT, p INT)");
        execute_statement("CREATE INDEX ev" + s + "_dev ON ev" + s + " (dev)");
        execute_statement("CREATE TABLE ed" + s + " (id INT, dev INT, p INT, PRIMARY KEY (id DESC))");
        execute_statement("CREATE INDEX ed" + s + "_dev ON ed" + s + " (dev)");
        execute_statement("CREATE TABLE ec" + s + " (a INT, b INT, c INT, PRIMARY KEY (a, b))");
        execute_statement("CREATE INDEX ec" + s + "_c ON ec" + s + " (c)");
        std::string values{};
        for(std::int32_t id = 0; id < 60; ++id) {
            // the rows are inserted in the order different from the primary key
            auto k = (id * 37) % 60;
            auto dev = k % 10 == 0 ? std::string{"NULL"} : std::to_string(k % 4);
            values += (id == 0 ? "" : ", ");
            values += "(" + std::to_string(k) + ", " + dev + ", " + std::to_string(id) + ")";
        }
        execute_statement("INSERT INTO ev" + s + " VALUES " + values);
        execute_statement("INSERT INTO ed" + s + " VALUES " + values);
        std::string c_values{};
        for(std::int32_t i = 0; i < 60; ++i) {
            auto k = (i * 37) % 60;
            c_values += (i == 0 ? "" : ", ");
            c_values += "(" + std::to_string(k / 10) + ", " + std::to_string(k % 10) + ", " + std::to_string(k % 3) + ")";
        }
        execute_statement("INSERT INTO ec" + s + " VALUES " + c_values);
    }

    // replaces `{}` in the query with the suffix
    static std::string with_suffix(std::string_view query, std::string_view suffix) {
        std::string ret{};
        for(std::size_t i = 0; i < query.size(); ++i) {
            if(query.substr(i, 2) == "{}") {
                ret += suffix;
                ++i;
                continue;
            }
            ret += query[i];
        }
        return ret;
    }

    std::vector<basic_record> query(std::string const& sql) {
        std::vector<basic_record> result{};
        if(GetParam()) {
            auto tx = utils::create_transaction(*db_, true, false);
            execute_query(sql, *tx, result);
            EXPECT_EQ(status::ok, tx->commit());
        } else {
            execute_query(sql, result);
        }
        return result;
    }

    static std::string to_string(std::vector<basic_record> const& records) {
        std::stringstream ss{};
        for(auto&& rec : records) {
            ss << rec << "\n";
        }
        return ss.str();
    }

    // verifies the query on the tables with the feature returns the same ordered result as the ones without it
    void verify(std::string_view query_template) {
        auto with = with_suffix(query_template, "_on");
        auto without = with_suffix(query_template, "_off");
        SCOPED_TRACE(with);
        auto expected = query(without);
        auto actual = query(with);
        EXPECT_FALSE(expected.empty());
        EXPECT_EQ(expected, actual) << "expected:\n" << to_string(expected) << "actual:\n" << to_string(actual);
    }

    std::string plan(std::string const& sql) {
        std::string ret{};
        explain_statement(sql, ret);
        return ret;
    }

    std::shared_ptr<configuration> cfg_{};  //NOLINT
};

INSTANTIATE_TEST_SUITE_P(
    ties,
    sql_ties_ordered_by_primary_key_test,
    ::testing::Bool(),
    [](auto const& info) {
        return std::string{info.param ? "rtx" : "occ"};
    }
);

TEST_P(sql_ties_ordered_by_primary_key_test, feature_changes_plan) {
    // make sure the comparison below is not trivial - the sort is removed with the feature
    auto q = "SELECT * FROM ev{} WHERE dev = 2 ORDER BY id LIMIT 5"sv;
    auto with = plan(with_suffix(q, "_on"));
    auto without = plan(with_suffix(q, "_off"));
    EXPECT_EQ(std::string::npos, with.find(R"("kind":"group")")) << with;
    EXPECT_NE(std::string::npos, without.find(R"("kind":"group")")) << without;
}

TEST_P(sql_ties_ordered_by_primary_key_test, equal_key) {
    verify("SELECT * FROM ev{} WHERE dev = 2 ORDER BY id LIMIT 5");
    verify("SELECT * FROM ev{} WHERE dev = 2 ORDER BY id LIMIT 100");
    verify("SELECT * FROM ev{} WHERE dev = 2 ORDER BY id");
    verify("SELECT * FROM ev{} WHERE dev = 3 AND p > 10 ORDER BY id LIMIT 3");
}

TEST_P(sql_ties_ordered_by_primary_key_test, null_key) {
    verify("SELECT * FROM ev{} WHERE dev IS NULL ORDER BY id LIMIT 3");
    verify("SELECT * FROM ev{} ORDER BY dev, id LIMIT 20");
}

TEST_P(sql_ties_ordered_by_primary_key_test, key_and_primary_key) {
    verify("SELECT * FROM ev{} ORDER BY dev, id LIMIT 30");
    verify("SELECT * FROM ev{} WHERE dev > 1 ORDER BY dev, id LIMIT 10");
    verify("SELECT * FROM ev{} ORDER BY dev, id");
}

TEST_P(sql_ties_ordered_by_primary_key_test, descending_primary_key) {
    verify("SELECT * FROM ed{} WHERE dev = 1 ORDER BY id DESC LIMIT 5");
    verify("SELECT * FROM ed{} ORDER BY dev, id DESC LIMIT 20");
}

TEST_P(sql_ties_ordered_by_primary_key_test, composite_primary_key) {
    verify("SELECT * FROM ec{} WHERE c = 1 ORDER BY a, b LIMIT 5");
    verify("SELECT * FROM ec{} ORDER BY c, a, b LIMIT 25");
}

}  // namespace jogasaki::testing
