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

#include <chrono>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/api/commit_option.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/create_commit_option.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using decimal_v = takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

/**
 * @brief testing durability callback using api
 */
class durability_callback_api_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->default_commit_response(commit_response_kind::propagated);
        cfg->profile_commits(true);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
    void test_commit_response();
};

using namespace std::string_view_literals;

TEST_F(durability_callback_api_test, occ_available) {
    utils::set_global_tx_option({false, true});
    utils::set_global_commit_option(api::commit_option{}.commit_response(commit_response_kind::available));
    test_commit_response();
}

TEST_F(durability_callback_api_test, occ_stored) {
    utils::set_global_tx_option({false, true});
    utils::set_global_commit_option(api::commit_option{}.commit_response(commit_response_kind::stored));
    test_commit_response();
}

TEST_F(durability_callback_api_test, ltx_available) {
    utils::set_global_tx_option({true, false});
    utils::set_global_commit_option(api::commit_option{}.commit_response(commit_response_kind::available));
    test_commit_response();
}

TEST_F(durability_callback_api_test, ltx_stored) {
    utils::set_global_tx_option({true, false});
    utils::set_global_commit_option(api::commit_option{}.commit_response(commit_response_kind::stored));
    test_commit_response();
}

TEST_F(durability_callback_api_test, occ_default) {
    utils::set_global_tx_option({false, true});
    utils::set_global_commit_option(api::commit_option{}.commit_response(commit_response_kind::undefined));
    test_commit_response();
}

void durability_callback_api_test::test_commit_response() {
    execute_statement("create table T (C0 int primary key)");
    constexpr std::size_t num_rows = 10;
    std::vector<std::chrono::nanoseconds> took(num_rows);
    using clock = std::chrono::system_clock;
    auto begin = clock::now();
    for(std::size_t i=0; i < num_rows; ++i) {
        execute_statement("INSERT INTO T VALUES ("+std::to_string(i)+")");
        auto end = clock::now();
        took[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
        begin = end;
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from T", result);
        ASSERT_EQ(num_rows, result.size());
    }
    for(std::size_t i=0; i < num_rows; ++i) {
        std::cerr << i << ":" << (took[i].count() / 1000) << " (us)" << std::endl;
    }
}

}
