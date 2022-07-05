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
#include <jogasaki/mock/basic_record.h>
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

class load_test :
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
        std::string_view sql,
        std::vector<std::string>& files,
        std::size_t max_records_per_file = -1
    ) {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable(sql, stmt));
        explain(*stmt);
        auto transaction = utils::create_transaction(*db_);
        auto& tx = *reinterpret_cast<impl::transaction*>(transaction->get());
        status s{};
        std::string message{"message"};
        std::atomic_bool run{false};
        test_channel ch{};
        ASSERT_TRUE(tx.execute_dump(
            maybe_shared_ptr{stmt.get()},
            maybe_shared_ptr{&ch},
            path(),
            [&](status st, std::string_view msg){
                s = st;
                message = msg;
                run.store(true);
            },
            max_records_per_file
        ));
        while(! run.load()) {}
        ASSERT_EQ(status::ok, s);
        ASSERT_TRUE(message.empty());
        auto& wrt = ch.writers_[0];
        ASSERT_TRUE(stmt->meta());
        auto m = create_file_meta();
        auto recs = deserialize_msg({wrt->data_.data(), wrt->size_}, *m->origin());
        ASSERT_LT(0, recs.size());
        for(auto&& x : recs) {
            files.emplace_back(static_cast<std::string>(x.get_value<accessor::text>(0)));
            LOG(INFO) << x;
        }
        EXPECT_TRUE(ch.all_writers_released());
        ASSERT_EQ(status::ok, tx.commit());
    }

    void test_load(
        std::vector<std::string> const& files, status expected = status::ok
    ) {
        auto transaction = utils::create_transaction(*db_);
        auto& tx = *reinterpret_cast<impl::transaction*>(transaction->get());
        status s{};
        std::string message{"message"};
        std::atomic_bool run{false};

        api::statement_handle prepared{};
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int8},
            {"p1", api::field_type_kind::float8},
        };
        ASSERT_EQ(status::ok, db_->prepare("INSERT INTO T0(C0, C1) VALUES (:p0, :p1)", variables, prepared));

        auto ps = api::create_parameter_set();
        ps->set_float8("p1", 1.0);
        ps->set_reference_column("p0", "C0");

        ASSERT_TRUE(tx.execute_load(
            prepared,
            std::shared_ptr{std::move(ps)},
            files,
            [&](status st, std::string_view msg){
                s = st;
                message = msg;
                run.store(true);
            }
        ));
        while(! run.load()) {}
        ASSERT_EQ(expected, s);
        if (expected == status::ok) {
            ASSERT_EQ(status::ok, tx.commit());
        } else {
            ASSERT_EQ(status::ok, tx.abort());
        }
    }
};

using namespace std::string_view_literals;

TEST_F(load_test, basic) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    std::vector<std::string> files{};
    test_dump("select * from T0", files);
    execute_statement( "DELETE FROM T0");
    test_load(files);
    {
        using kind = meta::field_type_kind;
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 ORDER BY C0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(1,1.0)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(2,1.0)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(3,1.0)), result[2]);
    }
}

TEST_F(load_test, wrong_file) {
    std::vector<std::string> files{"dummy.parquet"};
    test_load(files, status::err_aborted);
}

TEST_F(load_test, existing_file_and_missing_file) {
    // verify load failed with missing file and transaction abort, no records are loaded eventually
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    std::vector<std::string> files{};
    test_dump("select * from T0", files);
    execute_statement( "DELETE FROM T0");
    files.emplace_back("dummy.parquet");
    test_load(files, status::err_aborted);
    {
        using kind = meta::field_type_kind;
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 ORDER BY C0", result);
        ASSERT_EQ(0, result.size());
    }
}

}
