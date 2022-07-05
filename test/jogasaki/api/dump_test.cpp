/*
 * Copyright 2018-2020 tsurugi project.
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

#include <regex>
#include <future>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/data/any.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"
#include "../test_utils/temporary_folder.h"
#include <jogasaki/mock/test_channel.h>
#include <jogasaki/utils/msgbuf_utils.h>
#include <jogasaki/utils/create_tx.h>

#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"
#include "schema.pb.h"
#include "status.pb.h"

namespace jogasaki::api {

using namespace std::chrono_literals;
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::utils;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;
namespace sql = jogasaki::proto::sql;

inline std::shared_ptr<jogasaki::meta::external_record_meta> create_file_meta() {
    return std::make_shared<meta::external_record_meta>(
        std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{
                meta::field_type(meta::field_enum_tag<meta::field_type_kind::character>),
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
        return true;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->tasked_write(true);
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
        std::size_t max_records_per_file = -1,
        bool keep_files_on_error = false,
        status expected = status::ok
    ) {
        explain(stmt);
        auto transaction = utils::create_transaction(*db_);
        auto& tx = *reinterpret_cast<impl::transaction*>(transaction->get());
        status s{};
        std::string message{"message"};
        std::atomic_bool run{false};
        test_channel ch{};
        ASSERT_TRUE(tx.execute_dump(
            maybe_shared_ptr{&stmt},
            maybe_shared_ptr{&ch},
            path,
            [&](status st, std::string_view msg){
                s = st;
                message = msg;
                run.store(true);
            },
            max_records_per_file,
            keep_files_on_error
        ));
        while(! run.load()) {}
        ASSERT_EQ(expected, s);
        ASSERT_TRUE(message.empty());
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
        ASSERT_EQ(status::ok, tx.commit());
    }
    void test_dump(
        std::string_view sql,
        std::string_view path,
        std::size_t max_records_per_file = -1,
        bool keep_files_on_error = false,
        status expected = status::ok
    ) {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable(sql, stmt));
        test_dump(*stmt, path, max_records_per_file, keep_files_on_error, expected);
    }

    void test_dump(
        std::string_view sql,
        std::size_t max_records_per_file = -1
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


}
