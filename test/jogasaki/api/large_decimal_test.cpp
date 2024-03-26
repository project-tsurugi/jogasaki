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

#include <decimal.hh>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using triple = takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class large_decimal_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->prepare_test_tables(true);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(large_decimal_test, bad_calculation_in_decimal128) {
    // when IEEEContext(128) was used before, the result accidentally rounded
    execute_statement("create table t (c0 decimal (38))");
    execute_statement("insert into t values (cast('11111111111111111111111111111111111' as DECIMAL(38)))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c0 from t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::decimal>(
                std::tuple{meta::field_type{std::make_shared<meta::decimal_field_option>(38, 0)}},//TODO fix precision and scale to nullopt
                {static_cast<triple>(decimal::Decimal{"11111111111111111111111111111111111"})}
            )),
            result[0]
        );
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c0*8 from t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::decimal>(
                std::tuple{meta::field_type{std::make_shared<meta::decimal_field_option>(38, 0)}}, //TODO fix precision and scale to nullopt
                {static_cast<triple>(decimal::Decimal{"88888888888888888888888888888888888"})}
            )),
            result[0]
        );
    }
}

}  // namespace jogasaki::testing
