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

class dump_test :
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

TEST_F(dump_test, basic) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    test_dump("select * from T0");
}

TEST_F(dump_test, join) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    test_dump("select T00.C0 as T00C0, T01.C0 as T00C1 from T0 T00, T0 T01");
}

TEST_F(dump_test, types) {
    execute_statement( "INSERT INTO T20 (C0, C1, C2, C3, C4) VALUES (1, 11, 111.1, 1111.1, '11111111111111111111')");
    execute_statement( "INSERT INTO T20 (C0, C1, C2, C3, C4) VALUES (2, 22, 222.2, 2222.2, '22222222222222222222')");
    test_dump("select * from T20");
}

TEST_F(dump_test, binary) {
    execute_statement("CREATE TABLE T(PK INT NOT NULL PRIMARY KEY, C0 BINARY(3), C1 VARBINARY(3))");
    execute_statement("INSERT INTO T (PK) VALUES (0)");
    execute_statement("INSERT INTO T VALUES(1, x'01', x'0102')");
    test_dump("select * from T");
}

TEST_F(dump_test, empty_output) {
    test_dump("select * from T0", path(), -1, false, status::ok, true);
}

TEST_F(dump_test, large_output) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (0, 0.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (4, 40.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (5, 50.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (6, 60.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (7, 70.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (8, 80.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (9, 90.0)");
    test_dump(
        "select T00.C0 as T00C0, T01.C0 as T01C0, T02.C0 as T02C0, T03.C0 as T03C0, T04.C0 as T04C0 from T0 T00, T0 T01, T0 T02, T0 T03, T0 T04",
        10000
    );
}

TEST_F(dump_test, bad_path) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    test_dump("select * from T0", "/dummy_directory_name", -1, false, status::err_io_error);
}

TEST_F(dump_test, dump_error) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 0.0)");
    test_dump("select 20/C1 from T0", path(), -1, false, status::err_expression_evaluation_failure);
}

std::size_t dir_file_count(std::string path) {
    boost::filesystem::path p{path};
    if (! boost::filesystem::is_directory(p) || ! boost::filesystem::exists(p)) {
        return 0;
    }
    boost::filesystem::directory_iterator it{p};
    boost::filesystem::directory_iterator end{};
    std::size_t ret = 0;
    while(it != end) {
        if(boost::filesystem::is_regular_file(it->path())) {
            ++ret;
        }
        ++it;
    }
    return ret;
}

TEST_F(dump_test, dump_error_delete_files_on_failure) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 0.0)");
    test_dump("select 20/C1 from T0", path(), 1, false, status::err_expression_evaluation_failure);
    ASSERT_EQ(0, dir_file_count(path()));
}

TEST_F(dump_test, dump_error_keep_files_on_failure) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 0.0)");
    test_dump("select 20/C1 from T0", path(), 1, true, status::err_expression_evaluation_failure);
    ASSERT_EQ(2, dir_file_count(path()));
}

}  // namespace jogasaki::api
