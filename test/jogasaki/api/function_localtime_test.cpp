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

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using decimal_v = takatori::decimal::triple;
using date = takatori::datetime::date;
using time_of_day = takatori::datetime::time_of_day;
using time_point = takatori::datetime::time_point;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class function_localtime_test:
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

void set_tx_begin_ts(api::transaction_handle tx, transaction_context::clock::time_point ts) {
    auto& t = *reinterpret_cast<transaction_context*>(tx.get());
    t.start_time(ts);
}

TEST_F(function_localtime_test, at_the_begining_of_the_day) {
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 int)");
    execute_statement("insert into t values (1)");

    auto tx = utils::create_transaction(*db_);
    time_point tp{date{2000, 1, 1}, time_of_day{0, 0, 0}};
    set_tx_begin_ts(*tx, transaction_context::clock::time_point{tp.seconds_since_epoch()});
    execute_query("SELECT localtime FROM t", *tx, result);
    ASSERT_EQ(status::ok, tx->commit());
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::time_of_day>(std::tuple{time_of_day_type()}, {time_of_day{0, 0, 0}})), result[0]);
}

TEST_F(function_localtime_test, at_the_end_of_the_day) {
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 int)");
    execute_statement("insert into t values (1)");

    auto tx = utils::create_transaction(*db_);
    time_point tp{date{1999, 12, 31}, time_of_day{23, 59, 59}};
    set_tx_begin_ts(*tx, transaction_context::clock::time_point{tp.seconds_since_epoch()});
    execute_query("SELECT localtime FROM t", *tx, result);
    ASSERT_EQ(status::ok, tx->commit());
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::time_of_day>(std::tuple{time_of_day_type()}, {time_of_day{23, 59, 59}})), result[0]);
}

TEST_F(function_localtime_test, at_the_begining_of_the_day_with_offset) {
    global::config_pool()->zone_offset(9*60);

    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 int)");
    execute_statement("insert into t values (1)");

    auto tx = utils::create_transaction(*db_);
    time_point tp{date{1999, 12, 31}, time_of_day{15, 0, 0}};
    set_tx_begin_ts(*tx, transaction_context::clock::time_point{tp.seconds_since_epoch()});
    execute_query("SELECT localtime FROM t", *tx, result);
    ASSERT_EQ(status::ok, tx->commit());
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::time_of_day>(std::tuple{time_of_day_type()}, {time_of_day{0, 0, 0}})), result[0]);
}

TEST_F(function_localtime_test, at_the_end_of_the_day_with_offset) {
    global::config_pool()->zone_offset(9*60);

    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 int)");
    execute_statement("insert into t values (1)");

    auto tx = utils::create_transaction(*db_);
    time_point tp{date{1999, 12, 31}, time_of_day{14, 59, 59}};
    set_tx_begin_ts(*tx, transaction_context::clock::time_point{tp.seconds_since_epoch()});
    execute_query("SELECT localtime FROM t", *tx, result);
    ASSERT_EQ(status::ok, tx->commit());
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::time_of_day>(std::tuple{time_of_day_type()}, {time_of_day{23, 59, 59}})), result[0]);
}

}  // namespace jogasaki::testing
