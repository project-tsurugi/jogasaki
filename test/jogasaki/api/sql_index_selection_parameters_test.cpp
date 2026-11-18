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

#include <memory>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace std::string_view_literals;

/**
 * @brief tests that the parameters for the index selection of the compiler are passed from the configuration
 * @details the table has the primary key (w, d, id) and the secondary index (w, d, last_name, first_name). Changing
 * each parameter switches the index chosen by the compiler between the primary and the secondary indices.
 */
class sql_index_selection_parameters_test :
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
        execute_statement(
            "CREATE TABLE customer (w INT, d INT, id INT, last_name VARCHAR(16), first_name VARCHAR(16), balance INT, "
            "PRIMARY KEY (w, d, id))"
        );
        execute_statement("CREATE INDEX customer_secondary ON customer (w, d, last_name, first_name)");
    }

    void TearDown() override {
        db_teardown();
    }

    // returns true if the compiler chooses the secondary index for the query
    bool uses_secondary(std::string_view sql) {
        std::string plan{};
        explain_statement(sql, plan);
        return plan.find(R"("customer_secondary")") != std::string::npos;
    }

    std::shared_ptr<configuration> cfg_{};  //NOLINT
};

constexpr std::string_view equivalent_query =
    "SELECT * FROM customer WHERE w = 1 AND d = 2 AND last_name = 'a'";

TEST_F(sql_index_selection_parameters_test, selectivity_decay) {
    EXPECT_TRUE(uses_secondary(equivalent_query));
    cfg_->index_selectivity_decay(0.5);
    EXPECT_FALSE(uses_secondary(equivalent_query));
}

TEST_F(sql_index_selection_parameters_test, indirect_access_weight) {
    EXPECT_TRUE(uses_secondary(equivalent_query));
    cfg_->index_indirect_access_weight(20.0);
    EXPECT_FALSE(uses_secondary(equivalent_query));
}

TEST_F(sql_index_selection_parameters_test, equivalent_selectivity) {
    EXPECT_TRUE(uses_secondary(equivalent_query));
    cfg_->index_equivalent_selectivity(0.6);
    EXPECT_FALSE(uses_secondary(equivalent_query));
}

TEST_F(sql_index_selection_parameters_test, full_bound_selectivity) {
    auto query = "SELECT * FROM customer WHERE w = 1 AND d = 2 AND last_name >= 'a' AND last_name < 'b'"sv;
    EXPECT_TRUE(uses_secondary(query));
    cfg_->index_full_bound_selectivity(0.9);
    EXPECT_FALSE(uses_secondary(query));
}

TEST_F(sql_index_selection_parameters_test, half_bound_selectivity) {
    auto query = "SELECT * FROM customer WHERE w = 1 AND d = 2 AND last_name >= 'a'"sv;
    EXPECT_FALSE(uses_secondary(query));
    cfg_->index_half_bound_selectivity(0.2);
    EXPECT_TRUE(uses_secondary(query));
}

TEST_F(sql_index_selection_parameters_test, max_early_stop_limit_count) {
    auto query = "SELECT * FROM customer WHERE w = 1 AND d = 2 ORDER BY last_name, first_name LIMIT 100"sv;
    EXPECT_TRUE(uses_secondary(query));
    cfg_->index_max_early_stop_limit_count(10);
    EXPECT_FALSE(uses_secondary(query));
}

TEST_F(sql_index_selection_parameters_test, sort_cost) {
    // without LIMIT, the secondary index in the order is chosen only if the sort is estimated costly
    auto query = "SELECT * FROM customer WHERE w = 1 AND d = 2 ORDER BY last_name, first_name"sv;
    EXPECT_FALSE(uses_secondary(query));
    cfg_->index_sort_cost(100.0);
    EXPECT_TRUE(uses_secondary(query));
}

}  // namespace jogasaki::testing
