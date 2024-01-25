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

#include <future>
#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/data/any.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/io/dump_config.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/mock/test_channel.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/msgbuf_utils.h>
#include <jogasaki/utils/storage_data.h>

#include "../test_utils/temporary_folder.h"
#include "api_test_base.h"

namespace jogasaki::api {

using namespace std::chrono_literals;
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::utils;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using decimal_v = takatori::decimal::triple;

using takatori::util::unsafe_downcast;
using jogasaki::api::impl::get_impl;

inline std::shared_ptr<jogasaki::meta::external_record_meta> create_file_meta() {
    return std::make_shared<meta::external_record_meta>(
        std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{
                meta::field_type(std::make_shared<meta::character_field_option>()),
            },
            boost::dynamic_bitset<std::uint64_t>{1}.flip()
        ),
        std::vector<std::optional<std::string>>{"file_name"}
    );
}

class dump_arrow_test :
    public ::testing::Test,
    public testing::api_test_base {
public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->prepare_test_tables(true);
        db_setup(cfg);

        auto* impl = db_impl();
        add_benchmark_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());

        temporary_.prepare();
    }

    void TearDown() override {
        db_teardown();
        temporary_.clean();
    }

    void test_dump(
        api::executable_statement& stmt,
        std::string_view path,
        std::size_t max_records_per_file = 0,
        bool keep_files_on_error = false,
        status expected = status::ok,
        bool empty_output = false
    ) {
        explain(stmt);
        auto transaction = utils::create_transaction(*db_);
        auto tx = get_impl(*db_).find_transaction(*transaction);
        status s{};
        std::string message{"message"};
        std::atomic_bool run{false};
        test_channel ch{};
        io::dump_config opts{};
        opts.max_records_per_file_ = max_records_per_file;
        opts.keep_files_on_error_ = keep_files_on_error;
        opts.file_format_ = executor::io::dump_file_format_kind::arrow;
        ASSERT_TRUE(executor::execute_dump(
            get_impl(*db_),
            tx,
            maybe_shared_ptr{&stmt},
            maybe_shared_ptr{&ch},
            path,
            [&](status st, std::shared_ptr<error::error_info> info){
                s = st;
                message = (info ? info->message() : "");
                run.store(true);
            },
            opts
        ));
        while(! run.load()) {}
        ASSERT_EQ(expected, s);
        if(expected == status::ok) {
            ASSERT_TRUE(message.empty());
        } else {
            std::cerr << "error msg: " << message << std::endl;
        }
        if (empty_output) {
            ASSERT_TRUE(ch.writers_.empty());
            ASSERT_EQ(status::ok, executor::commit(get_impl(*db_), tx));
            return;
        }
        ASSERT_FALSE(ch.writers_.empty());
        auto& wrt = ch.writers_[0];
        ASSERT_TRUE(stmt.meta());
        auto m = create_file_meta();
        auto recs = deserialize_msg({wrt->data_.data(), wrt->size_}, *m->origin());
        if(expected == status::ok) {
            ASSERT_LT(0, recs.size());
        }
        for(auto&& x : recs) {
            LOG(INFO) << x;
        }
        EXPECT_TRUE(ch.all_writers_released());
        ASSERT_EQ(status::ok, executor::commit(get_impl(*db_), tx));
    }
    void test_dump(
        std::string_view sql,
        std::string_view path,
        std::size_t max_records_per_file = 0,
        bool keep_files_on_error = false,
        status expected = status::ok,
        bool empty_output = false
    ) {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable(sql, stmt));
        test_dump(*stmt, path, max_records_per_file, keep_files_on_error, expected, empty_output);
    }

    void test_dump(
        std::string_view sql,
        std::size_t max_records_per_file = 0
    ) {
        return test_dump(sql, path(), max_records_per_file);
    }
};

using namespace std::string_view_literals;

TEST_F(dump_arrow_test, basic) {
    execute_statement("CREATE TABLE T(C0 BIGINT NOT NULL PRIMARY KEY, C1 DOUBLE)");
    execute_statement( "INSERT INTO T (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T (C0, C1) VALUES (3, 30.0)");
    test_dump("select * from T");
}

TEST_F(dump_arrow_test, int_float_types) {
    execute_statement("CREATE TABLE T(PK INT NOT NULL PRIMARY KEY, C0 INT, C1 BIGINT, C2 REAL, C3 DOUBLE)");
    execute_statement("INSERT INTO T (PK) VALUES (0)");
    execute_statement("INSERT INTO T VALUES (1, 11, 111, 11.11, 111.11)");
    execute_statement("INSERT INTO T VALUES (2, 22, 222, 22.22, 222.22)");
    execute_statement("INSERT INTO T VALUES (3, 33, 333, 33.33, 333.33)");
    test_dump("select * from T");
}

TEST_F(dump_arrow_test, char_types) {
    execute_statement("CREATE TABLE T(PK INT NOT NULL PRIMARY KEY, C0 CHAR(5), C1 VARCHAR(5))");
    execute_statement("INSERT INTO T (PK) VALUES (0)");
    execute_statement("INSERT INTO T VALUES (1, '111', '111')");
    execute_statement("INSERT INTO T VALUES (2, '222', '222')");
    execute_statement("INSERT INTO T VALUES (3, '333', '333')");
    test_dump("select * from T");
}

TEST_F(dump_arrow_test, decimal_types) {
    execute_statement("CREATE TABLE T(PK INT NOT NULL PRIMARY KEY, C0 DECIMAL(3), C1 DECIMAL(5,3), C2 DECIMAL(10,5))");
    execute_statement("INSERT INTO T (PK) VALUES (0)");
    execute_statement("INSERT INTO T VALUES (1, CAST('111' AS DECIMAL(3)), CAST('11.111' AS DECIMAL(5,3)), CAST('11111.11111' AS DECIMAL(10,5)))");
    execute_statement("INSERT INTO T VALUES (2, CAST('222' AS DECIMAL(3)), CAST('22.222' AS DECIMAL(5,3)), CAST('22222.22222' AS DECIMAL(10,5)))");
    execute_statement("INSERT INTO T VALUES (3, CAST('333' AS DECIMAL(3)), CAST('33.333' AS DECIMAL(5,3)), CAST('33333.33333' AS DECIMAL(10,5)))");
    test_dump("select * from T");
}

TEST_F(dump_arrow_test, temporal_types) {
    execute_statement("CREATE TABLE T(PK INT NOT NULL PRIMARY KEY, C0 DATE, C1 TIME, C2 TIMESTAMP)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::date},
        {"p1", api::field_type_kind::time_of_day},
        {"p2", api::field_type_kind::time_point},
    };
    auto d2000_1_1 = date_v{2000, 1, 1};
    auto t12_0_0 = time_of_day_v{12, 0, 0};
    auto tp2000_1_1_12_0_0 = time_point_v{d2000_1_1, t12_0_0};
    auto ps = api::create_parameter_set();
    ps->set_date("p0", d2000_1_1);
    ps->set_time_of_day("p1", t12_0_0);
    ps->set_time_point("p2", tp2000_1_1_12_0_0);
    execute_statement("INSERT INTO T (PK) VALUES (0)");
    execute_statement("INSERT INTO T VALUES (1, :p0, :p1, :p2)", variables, *ps);
    test_dump("select * from T");
}
TEST_F(dump_arrow_test, many_types) {
    execute_statement("CREATE TABLE T("
                      "PK INT NOT NULL PRIMARY KEY,"
                      "C0 INT,"
                      "C1 BIGINT,"
                      "C2 REAL,"
                      "C3 DOUBLE,"
                      "C4 CHAR(5),"
                      "C5 VARCHAR(5),"
                      "C6 DECIMAL(3),"
                      "C7 DECIMAL(5,3),"
                      "C8 DECIMAL(10,5),"
                      "C9 DATE,"
                      "C10 TIME,"
                      "C11 TIMESTAMP"
                      ")");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::date},
        {"p1", api::field_type_kind::time_of_day},
        {"p2", api::field_type_kind::time_point},
    };
    auto d2000_1_1 = date_v{2000, 1, 1};
    auto t12_0_0 = time_of_day_v{12, 0, 0};
    auto tp2000_1_1_12_0_0 = time_point_v{d2000_1_1, t12_0_0};
    auto ps = api::create_parameter_set();
    ps->set_date("p0", d2000_1_1);
    ps->set_time_of_day("p1", t12_0_0);
    ps->set_time_point("p2", tp2000_1_1_12_0_0);
    execute_statement("INSERT INTO T (PK) VALUES (0)");
    execute_statement("INSERT INTO T VALUES (1, 1, 11, 11.1, 11.11, '111', '111', CAST('111' AS DECIMAL(3)), CAST('11.111' AS DECIMAL(5,3)), CAST('11111.11111' AS DECIMAL(10,5)), :p0, :p1, :p2)", variables, *ps);
    test_dump("select * from T");
}

}  // namespace jogasaki::api
