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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cxxabi.h>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using decimal_v = takatori::decimal::triple;

// verify iceaxe test assigning max+1 read with primary key
class validate_user_scenario6_test :
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

// temporarily disabled testing to avoid disturbing CI
TEST_F(validate_user_scenario6_test, DISABLED_assign_max_plus_one) {
    if(kvs::implementation_id() == "memory") {
        // this requres cc detect unique constraint violation and serialization error
        GTEST_SKIP();
    }
    static constexpr std::size_t parallelism = 30;
    // test for occ after shirakami scan is fixed TODO
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table test(foo int, bar bigint, zzz varchar(10), primary key(foo))");
    execute_statement("create table test2(foo int, bar bigint, zzz varchar(10), primary key(foo))");
    execute_statement("insert into test(foo, bar, zzz)values(0, 0, '0')");
    std::vector<std::future<void>> list{};
    std::atomic_size_t count_unique_const_violation = 0;
    std::atomic_size_t count_serialization_failure = 0;
    for(std::size_t i=0; i < parallelism; ++i) {
        auto f = std::async(std::launch::async, [&, this]() {
            while(true) {
                auto tx1 = utils::create_transaction(*db_);
                std::int32_t next_key{};
                {
                    auto ps = api::create_parameter_set();
                    std::unordered_map<std::string, api::field_type_kind> variables{};
                    std::vector<mock::basic_record> result{};
                    execute_query("select max(foo) + 1 as foo from test", variables, *ps, *tx1, result);
                    ASSERT_EQ(1, result.size());
                    next_key = result[0].get_value<std::int32_t>(0);
                }
                {
                    auto sql = "insert into test(foo, bar, zzz)values(" + std::to_string(next_key) + ", " + std::to_string(next_key) +
                        ",  '" + std::to_string(next_key) + "')";
                    api::statement_handle prepared{};
                    std::unordered_map<std::string, api::field_type_kind> variables{};
                    ASSERT_EQ(status::ok, db_->prepare(sql, variables, prepared));
                    std::unique_ptr<api::executable_statement> stmt{};

                    api::impl::parameter_set params{};
                    ASSERT_EQ(status::ok, db_->resolve(prepared, maybe_shared_ptr{&params}, stmt));
                    explain(*stmt);
                    auto res = tx1->execute(*stmt);
                    ASSERT_EQ(status::ok, db_->destroy_statement(prepared));
                    if (res == status::err_unique_constraint_violation) {
                        ++count_unique_const_violation;
                        continue;
                    }
                }
                {
                    auto res2 = tx1->commit();
                    if (res2 == status::err_serialization_failure) {
                        ++count_serialization_failure;
                        continue;
                    }
                }
                break;
            }
        });
        list.emplace_back(std::move(f));
    }
    for(auto&& f : list) {
        f.get();
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from test order by foo", result);
        ASSERT_EQ(parallelism+1, result.size());
    }
    // verify retry count are not surprisingly high
    std::cerr << "count unique constraint violation: " << count_unique_const_violation << std::endl;
    std::cerr << "count serialization failure: " << count_serialization_failure << std::endl;
    ASSERT_LT(count_unique_const_violation, 10000);
    ASSERT_LT(count_serialization_failure, 10000);
}

}
