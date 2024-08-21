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

using takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class sql_many_repeated_elements_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->trace_external_log(false);
        cfg->external_log_explain(false);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(sql_many_repeated_elements_test, many_elements_in_IN) {
    constexpr std::size_t N = 5000;
    constexpr std::size_t M = N/1000;
    execute_statement("CREATE TABLE t (c0 int)");
    for(std::size_t i=0; i<M; ++i) {
        execute_statement("INSERT INTO t VALUES ("+std::to_string(i)+")");
    }
    std::vector<mock::basic_record> result{};
    std::stringstream ss{};
    bool first = true;
    for(std::size_t i=0; i<N; ++i) {
        if(! first) {
            ss << ",";
        }
        ss << std::to_string(i);
        first = false;
    }

    execute_query("SELECT count(*) FROM t WHERE c0 in ("+ss.str()+")", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int8>(M)), result[0]);
}

TEST_F(sql_many_repeated_elements_test, many_elements_in_IN_exceeding_max) {
    constexpr std::size_t N = 5001;
    execute_statement("CREATE TABLE t (c0 int)");
    std::vector<mock::basic_record> result{};
    std::stringstream ss{};
    bool first = true;
    for(std::size_t i=0; i<N; ++i) {
        if(! first) {
            ss << ",";
        }
        ss << std::to_string(i);
        first = false;
    }

    test_stmt_err("SELECT count(*) FROM t WHERE c0 in ("+ss.str()+")", error_code::syntax_exception);
}
}
