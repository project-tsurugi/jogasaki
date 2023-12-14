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
#include <jogasaki/executor/io/dump_option.h>
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
                meta::field_type(meta::field_enum_tag<meta::field_type_kind::character>),
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
        std::size_t max_records_per_file = -1,
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
            max_records_per_file,
            keep_files_on_error,
            executor::io::dump_file_format_kind::arrow
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
        std::size_t max_records_per_file = -1,
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
        std::size_t max_records_per_file = -1
    ) {
        return test_dump(sql, path(), max_records_per_file);
    }
};

using namespace std::string_view_literals;

TEST_F(dump_arrow_test, basic) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    test_dump("select * from T0");
}

}  // namespace jogasaki::api
