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

    void test_bootstrap(std::vector<std::vector<std::size_t>> block_def_list);
    void create_test_file(boost::filesystem::path const& p, std::vector<std::size_t> record_counts, std::size_t initial) {
        auto rec = mock::create_nullable_record<kind::int8, kind::float8>();
        auto writer = file::parquet_writer::open(
            std::make_shared<meta::external_record_meta>(
                rec.record_meta(),
                std::vector<std::optional<std::string>>{"C0", "C1"}
            ), p.string());
        ASSERT_TRUE(writer);
        std::size_t pos = 0;
        std::size_t ind = initial;
        for(auto record_count : record_counts) {
            for(std::size_t i=0; i< record_count; ++i) {
                auto rec = mock::create_nullable_record<kind::int8, kind::float8>(ind, ind);
                writer->write(rec.ref());
                ++ind;
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
    create_test_file(p1, {2, 1}, 3);

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
        batch_execution_info{
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
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(1)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(2)), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(3)), result[3]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(4)), result[4]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(5)), result[5]);
    }
    ASSERT_TRUE(called);
    EXPECT_EQ(2, file_release_count);
    EXPECT_EQ(4, block_release_count);
}

TEST_F(batch_executor_test, bootstrap) {
    test_bootstrap({{1, 2}, {2, 1}});
}

TEST_F(batch_executor_test, variation1) {
    test_bootstrap({{1, 2, 3}, {1}, {1, 3}});
}

TEST_F(batch_executor_test, variation2) {
    test_bootstrap({{100}});
}

TEST_F(batch_executor_test, variation3) {
    test_bootstrap({{1}, {1}, {1}, {1}, {1}});
}

TEST_F(batch_executor_test, many_files) {
    std::size_t file_count = 100;
    std::vector<std::vector<std::size_t>> defs{};
    defs.reserve(file_count);
    for(std::size_t i=0; i < file_count; ++i) {
        defs.emplace_back(std::vector<std::size_t>{i});
    }
    test_bootstrap(std::move(defs));
}

TEST_F(batch_executor_test, many_blocks) {
    std::size_t block_count = 100;
    std::vector<std::vector<std::size_t>> defs{};
    std::vector<std::size_t> blocks{};
    blocks.reserve(block_count);
    for(std::size_t i=0; i < block_count; ++i) {
        blocks.emplace_back(i);
    }
    defs.emplace_back(std::move(blocks));
    test_bootstrap(std::move(defs));
}

// TODO handle session limit error
TEST_F(batch_executor_test, DISABLED_many_files_and_blocks) {
    std::size_t block_count = 50;
    std::size_t file_count = 50;
    std::vector<std::vector<std::size_t>> defs{};
    std::vector<std::size_t> blocks{};
    blocks.reserve(block_count);
    for(std::size_t i=0; i < block_count; ++i) {
        blocks.emplace_back(i);
    }
    for(std::size_t i=0; i < file_count; ++i) {
        defs.emplace_back(blocks);
    }
    test_bootstrap(std::move(defs));
}
void batch_executor_test::test_bootstrap(std::vector<std::vector<std::size_t>> block_def_list) {
    execute_statement("CREATE TABLE TT (C0 BIGINT)");

    std::size_t file_count = block_def_list.size();
    boost::filesystem::path d{path()};
    std::vector<std::string> files{};
    std::size_t statement_count = 0;
    std::size_t block_count = 0;
    for(std::size_t i=0; i < file_count; ++i) {
        auto file = d / ("simple"+std::to_string(i)+".parquet");
        create_test_file(file, block_def_list[i], statement_count);
        for(auto&& e : block_def_list[i]) {
            statement_count += e;
        }
        block_count += block_def_list[i].size();
        files.emplace_back(file.string());
    }

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
        files,
        batch_execution_info{
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
//                std::cerr << "release file:" << arg << std::endl;
                    ++file_release_count;
                },
                [&](batch_block_executor* arg) {
//                std::cerr << "release block:" << arg << std::endl;
                    ++block_release_count;
                }
            }
        }
    );
    root->bootstrap();

    impl->scheduler()->wait_for_progress(scheduler::job_context::undefined_id);

    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TT ORDER BY C0", result);
        ASSERT_EQ(statement_count, result.size());
        for(std::size_t i=0; i < statement_count; ++i) {
            EXPECT_EQ((mock::create_nullable_record<kind::int8>(i)), result[i]);
        }
    }
    ASSERT_TRUE(called);
    EXPECT_EQ(file_count, file_release_count);
    EXPECT_EQ(block_count, block_release_count);
}

}
