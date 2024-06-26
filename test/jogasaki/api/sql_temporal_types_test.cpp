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

#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace std::chrono_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using takatori::decimal::triple;
using takatori::datetime::date;
using takatori::datetime::time_of_day;
using takatori::datetime::time_point;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class sql_temporal_types_test :
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

using jogasaki::accessor::text;

TEST_F(sql_temporal_types_test, timestamp) {
    // there was an issue with timestamp close to 0000-00-00
    execute_statement("CREATE TABLE T (C0 TIMESTAMP, C1 TIMESTAMP WITH TIME ZONE)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::time_point},
        {"p1", api::field_type_kind::time_point}, //TODO with time zone
    };
    auto tod = time_of_day{0, 2, 48, 91383000ns};
    auto tp = time_point{date{1, 1, 1}, tod};
    auto ps = api::create_parameter_set();
    ps->set_time_point("p0", tp);
    ps->set_time_point("p1", tp);
    execute_statement( "INSERT INTO T (C0, C1) VALUES (:p0, :p1)", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(1, result.size());
        auto tptz = meta::field_type{std::make_shared<meta::time_point_field_option>(true)};
        auto tpt = meta::field_type{std::make_shared<meta::time_point_field_option>(false)};
        EXPECT_EQ((mock::typed_nullable_record<kind::time_point, kind::time_point>(
            std::tuple{tpt, tptz}, { tp, tp }
        )), result[0]);
    }
}

}  // namespace jogasaki::testing
