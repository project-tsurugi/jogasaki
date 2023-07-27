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
#include <jogasaki/executor/batch/batch_executor.h>

#include <xmmintrin.h>
#include <gtest/gtest.h>
#include <jogasaki/test_utils/temporary_folder.h>

#include <jogasaki/executor/file/parquet_writer.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/api/api_test_base.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/executor/batch/batch_file_executor.h>
#include <jogasaki/executor/batch/batch_block_executor.h>

namespace jogasaki::executor::batch {

using kind = meta::field_type_kind;
using accessor::text;

using namespace std::chrono_literals;

class batch_executor_test :
    public ::testing::Test,
    public testing::api_test_base {

public:

    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
        auto* impl = db_impl();
        temporary_.prepare();
    }

    void TearDown() override {
        db_teardown();
        temporary_.clean();
    }

    void create_test_file(boost::filesystem::path const& p, std::vector<std::size_t> record_counts, std::size_t file_index) {
        auto rec = mock::create_nullable_record<kind::int8, kind::float8>();
        auto writer = file::parquet_writer::open(
            std::make_shared<meta::external_record_meta>(
                rec.record_meta(),
                std::vector<std::optional<std::string>>{"C0", "C1"}
            ), p.string());
        ASSERT_TRUE(writer);
        std::size_t pos = 0;
        for(auto record_count : record_counts) {
            for(std::size_t i=0; i< record_count; ++i) {
                auto j = file_index*record_count + i;
                auto rec = mock::create_nullable_record<kind::int8, kind::float8>(j*10, j*100.0);
                writer->write(rec.ref());
            }
            if(pos != record_counts.size()-1) { // skip if last
                writer->new_row_group();
            }
            ++pos;
        }
        writer->close();
        ASSERT_LT(0, boost::filesystem::file_size(p));
    }

    std::string path() {
        return temporary_.path();
    }

    test::temporary_folder temporary_{};  //NOLINT

};

TEST_F(batch_executor_test, simple) {
    execute_statement("CREATE TABLE TT (C0 BIGINT)");

    boost::filesystem::path d{path()};
    auto p0 = d / "simple0.parquet";
    auto p1 = d / "simple1.parquet";
    create_test_file(p0, {1, 2}, 0);
    create_test_file(p1, {2, 1}, 0);

    auto* impl = db_impl();
    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
    };
    ASSERT_EQ(status::ok, db_->prepare("INSERT INTO TT VALUES (:p0)", variables, prepared));

    auto ps = api::create_parameter_set();
    ps->set_reference_column("p0", "C0");

    std::atomic_bool called = false;
    std::atomic_size_t file_release_count = 0;
    std::atomic_size_t block_release_count = 0;
    auto root = std::make_shared<batch_executor>(
        std::vector<std::string>{p0.string(), p1.string()},
        prepared,
        std::shared_ptr{std::move(ps)},
        reinterpret_cast<api::impl::database*>(db_.get()),
        [&](){
            called = true;
        },
        batch_executor_option{
            batch_executor_option::undefined,
            batch_executor_option::undefined,
            [&](batch_file_executor* arg) {
                std::cerr << "release file:" << arg << std::endl;
                ++file_release_count;
            },
            [&](batch_block_executor* arg) {
                std::cerr << "release block:" << arg << std::endl;
                ++block_release_count;
            }
        }
    );

    auto&& [s0, f0] = root->next_file();
    auto&& [s1, f1] = root->next_file();
    auto&& [s2, f2] = root->next_file();
    ASSERT_TRUE(s2);
    ASSERT_FALSE(f2);

    {
        ASSERT_EQ(2, f0->block_count());
        auto&& [s0, b0] = f0->next_block();
        auto&& [s1, b1] = f0->next_block();
        auto&& [s2, b2] = f0->next_block();
        ASSERT_TRUE(s0);
        ASSERT_TRUE(s1);
        ASSERT_TRUE(s2);
        ASSERT_FALSE(b2);
        b0->execute_statement();
        b1->execute_statement();
    }
    {
        ASSERT_EQ(2, f1->block_count());
        auto&& [s0, b0] = f1->next_block();
        auto&& [s1, b1] = f1->next_block();
        auto&& [s2, b2] = f1->next_block();
        ASSERT_TRUE(s0);
        ASSERT_TRUE(s1);
        ASSERT_TRUE(s2);
        ASSERT_FALSE(b2);
        b0->execute_statement();
        b1->execute_statement();
    }

    impl->scheduler()->wait_for_progress(scheduler::job_context::undefined_id);

    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TT ORDER BY C0", result);
        ASSERT_EQ(6, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[3]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(10)), result[4]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(10)), result[5]);
    }
    ASSERT_TRUE(called);
    EXPECT_EQ(2, file_release_count);
    EXPECT_EQ(4, block_release_count);
}

TEST_F(batch_executor_test, bootstrap) {
    execute_statement("CREATE TABLE TT (C0 BIGINT)");

    boost::filesystem::path d{path()};
    auto p0 = d / "simple0.parquet";
    auto p1 = d / "simple1.parquet";
    create_test_file(p0, {1, 2}, 0);
    create_test_file(p1, {2, 1}, 0);

    auto* impl = db_impl();
    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
    };
    ASSERT_EQ(status::ok, db_->prepare("INSERT INTO TT VALUES (:p0)", variables, prepared));

    auto ps = api::create_parameter_set();
    ps->set_reference_column("p0", "C0");

    std::atomic_bool called = false;
    std::atomic_size_t file_release_count = 0;
    std::atomic_size_t block_release_count = 0;
    auto root = std::make_shared<batch_executor>(
        std::vector<std::string>{p0.string(), p1.string()},
        prepared,
        std::shared_ptr{std::move(ps)},
        reinterpret_cast<api::impl::database*>(db_.get()),
        [&](){
            called = true;
        },
        batch_executor_option{
            batch_executor_option::undefined,
            batch_executor_option::undefined,
            [&](batch_file_executor* arg) {
                std::cerr << "release file:" << arg << std::endl;
                ++file_release_count;
            },
            [&](batch_block_executor* arg) {
                std::cerr << "release block:" << arg << std::endl;
                ++block_release_count;
            }
        }
    );
    root->bootstrap();

    impl->scheduler()->wait_for_progress(scheduler::job_context::undefined_id);

    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TT ORDER BY C0", result);
        ASSERT_EQ(6, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[3]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(10)), result[4]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(10)), result[5]);
    }
    ASSERT_TRUE(called);
    EXPECT_EQ(2, file_release_count);
    EXPECT_EQ(4, block_release_count);
}

}

