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
#include <chrono>
#include <cstddef>
#include <optional>
#include <type_traits>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/api_test_base.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/file/loader.h>
#include <jogasaki/executor/file/parquet_writer.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/temporary_folder.h>
#include <jogasaki/utils/create_tx.h>

namespace jogasaki::executor::file {

using kind = meta::field_type_kind;
using accessor::text;

using namespace std::chrono_literals;

class loader_test :
    public ::testing::Test,
    public testing::api_test_base {

public:

    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->single_thread(false);
        cfg->prepare_test_tables(true);
        db_setup(cfg);
        auto* impl = db_impl();
        temporary_.prepare();
    }

    void TearDown() override {
        db_teardown();
        temporary_.clean();
    }

    std::string path() {
        return temporary_.path();
    }

    test::temporary_folder temporary_{};  //NOLINT

    void test_load(std::vector<std::string> const& files, std::shared_ptr<loader>& ldr, std::size_t bulk_size = 10000, bool expect_error = false, std::unique_ptr<api::parameter_set> ps = nullptr) {
        auto* impl = db_impl();
        api::statement_handle prepared{};
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int8},
            {"p1", api::field_type_kind::float8},
        };
        ASSERT_EQ(status::ok, db_->prepare("INSERT INTO T0(C0, C1) VALUES (:p0, :p1)", variables, prepared));

        if(! ps) {
            ps = api::create_parameter_set();
            ps->set_float8("p1", 1000.0);
            ps->set_reference_column("p0", "C0");
        }
        auto trans = utils::create_transaction(*db_);

        auto d = dynamic_cast<api::impl::database*>(db_.get());
        auto tx = d->find_transaction(*trans);
        ldr = std::make_shared<loader>(
            files,
            prepared,
            std::shared_ptr{std::move(ps)},
            tx,
            *d,
            bulk_size
        );

        loader_result res{};
        while((res = (*ldr)()) == loader_result::running) {
            impl->scheduler()->wait_for_progress(scheduler::job_context::undefined_id);
        }
        if (expect_error) {
            ASSERT_EQ(loader_result::error, res);
            ASSERT_NE(status::ok, ldr->error_info().first);
            trans->abort(); // tx already aborted on error. This is to verify repeat does no harm.
        } else {
            ASSERT_EQ(loader_result::ok, res);
            ASSERT_EQ(status::ok, ldr->error_info().first);
            trans->commit();
        }
    }
};

void create_test_file(boost::filesystem::path const& p, std::size_t record_count, std::size_t file_index) {
    auto rec = mock::create_nullable_record<kind::int8, kind::float8>();
    parquet_writer_option opt{};
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1"}
        ), p.string(), opt);
    ASSERT_TRUE(writer);
    for(std::size_t i=0; i< record_count; ++i) {
        auto j = file_index*record_count + i;
        auto rec = mock::create_nullable_record<kind::int8, kind::float8>(j*10, j*100.0);
        writer->write(rec.ref());
    }
    writer->close();
    ASSERT_LT(0, boost::filesystem::file_size(p));
}

TEST_F(loader_test, simple) {
    boost::filesystem::path p{path()};
    p = p / "simple.parquet";
    create_test_file(p, 2, 0);
    std::shared_ptr<loader> ldr{};
    test_load(std::vector<std::string>{p.string()}, ldr);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(0,1000.0)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(10,1000.0)), result[1]);
    }
    EXPECT_EQ(2, ldr->records_loaded());

}

TEST_F(loader_test, multiple_files) {
    std::vector<std::string> files{};
    for(std::size_t i=0; i < 10; ++i) {
        boost::filesystem::path p0{path()};
        p0 = p0 / (std::string{"multiple_files"}+std::to_string(i)+".parquet");
        create_test_file(p0, 2, i);
        files.emplace_back(p0.string());
    }
    std::shared_ptr<loader> ldr{};
    test_load(files, ldr);

    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(20, result.size());
    }
    EXPECT_EQ(20, ldr->records_loaded());
}

TEST_F(loader_test, multiple_read) {
    boost::filesystem::path p{path()};
    p = p / "multiple_read.parquet";
    create_test_file(p, 10, 0);
    std::shared_ptr<loader> ldr{};
    test_load(std::vector<std::string>{p.string()}, ldr, 3);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(10, result.size());
    }
    EXPECT_EQ(10, ldr->records_loaded());
}

TEST_F(loader_test, dummy_file) {
    std::shared_ptr<loader> ldr{};
    test_load(std::vector<std::string>{"dummy.parquet"}, ldr, 3, true);
}

TEST_F(loader_test, empty_file_name) {
    std::shared_ptr<loader> ldr{};
    test_load(std::vector<std::string>{""}, ldr, 3, true);
}

TEST_F(loader_test, bad_reference_column_name) {
    boost::filesystem::path p{path()};
    p = p / "bad_reference_column_name.parquet";
    create_test_file(p, 2, 0);

    std::shared_ptr<loader> ldr{};
    auto ps = api::create_parameter_set();
    ps->set_float8("p1", 1000.0);
    ps->set_reference_column("p0", "dummy");
    test_load(std::vector<std::string>{p.string()}, ldr, 3, true, std::move(ps));
}

TEST_F(loader_test, bad_reference_column_index) {
    boost::filesystem::path p{path()};
    p = p / "bad_reference_column_index.parquet";
    create_test_file(p, 2, 0);

    std::shared_ptr<loader> ldr{};
    auto ps = api::create_parameter_set();
    ps->set_float8("p1", 1000.0);
    ps->set_reference_column("p0", 100);
    test_load(std::vector<std::string>{p.string()}, ldr, 3, true, std::move(ps));
}

TEST_F(loader_test, extra_parameter) {
    // test extra parameter is ignored
    boost::filesystem::path p{path()};
    p = p / "extra_parameter.parquet";
    create_test_file(p, 2, 0);
    std::shared_ptr<loader> ldr{};
    auto ps = api::create_parameter_set();
    ps->set_float8("p1", 1000.0);
    ps->set_reference_column("p0", "C0");
    ps->set_reference_column("dummy", "bad"); // extra parameter not used in statement
    test_load(std::vector<std::string>{p.string()}, ldr, 3, false, std::move(ps));
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(0,1000.0)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(10,1000.0)), result[1]);
    }
    EXPECT_EQ(2, ldr->records_loaded());
}
}

