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
#include <functional>
#include <iostream>
#include <optional>
#include <string_view>
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
#include <jogasaki/configuration.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/batch/batch_block_executor.h>
#include <jogasaki/executor/batch/batch_execution_info.h>
#include <jogasaki/executor/batch/batch_execution_state.h>
#include <jogasaki/executor/batch/batch_executor.h>
#include <jogasaki/executor/batch/batch_executor_option.h>
#include <jogasaki/executor/batch/batch_file_executor.h>
#include <jogasaki/executor/file/parquet_writer.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/temporary_folder.h>

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

    void test_bootstrap(
        std::vector<std::vector<std::size_t>> block_def_list,
        std::size_t max_concurrent_files = batch_executor_option::undefined,
        std::size_t max_concurrent_blocks_per_file = batch_executor_option::undefined
    );
    void test_error(
        std::vector<std::vector<std::size_t>> block_def_list,
        status expected,
        std::function<void(std::size_t, std::size_t, std::size_t, std::size_t&)> customize_value = {},
        std::size_t max_concurrent_files = batch_executor_option::undefined,
        std::size_t max_concurrent_blocks_per_file = batch_executor_option::undefined
    );
    void create_test_file(
        boost::filesystem::path const& p,
        std::vector<std::size_t> record_counts,
        std::size_t initial,
        std::function<void(std::size_t block_index, std::size_t statement_index, std::size_t& value)> customize_value = {}
    ) {
        auto rec = mock::create_nullable_record<kind::int8, kind::float8>();
        auto writer = file::parquet_writer::open(
            std::make_shared<meta::external_record_meta>(
                rec.record_meta(),
                std::vector<std::optional<std::string>>{"C0", "C1"}
            ), p.string());
        ASSERT_TRUE(writer);
        std::size_t pos = 0;
        std::size_t ind = initial;
        std::size_t block_index = 0;
        for(auto record_count : record_counts) {
            std::size_t statement_index = 0;
            for(std::size_t i=0; i< record_count; ++i) {
                if(customize_value) {
                    customize_value(block_index, statement_index, ind);
                }
                auto rec = mock::create_nullable_record<kind::int8, kind::float8>(ind, ind);
                writer->write(rec.ref());
                ++ind;
                ++statement_index;
            }
            if(pos != record_counts.size()-1) { // skip if last block
                writer->new_row_group();
                ++block_index;
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
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
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
    auto root = batch_executor::create_batch_executor(
        std::vector<std::string>{p0.string(), p1.string()},
        batch_execution_info{
            prepared,
            std::shared_ptr{std::move(ps)},
            reinterpret_cast<api::impl::database*>(db_.get()),
            [&](){
                called = true;
            },
            batch_executor_option{
                [&](batch_file_executor* arg) {
//                    std::cerr << "release file:" << arg << std::endl;
                    ++file_release_count;
                },
                [&](batch_block_executor* arg) {
//                    std::cerr << "release block:" << arg << std::endl;
                    ++block_release_count;
                }
            }
        }
    );

    auto&& [s0, f0] = root->next_file();
    auto&& [s1, f1] = root->next_file();
    auto&& [s2, f2] = root->next_file();
    ASSERT_TRUE(s0);
    ASSERT_TRUE(s1);
    ASSERT_TRUE(s2);
    ASSERT_TRUE(f0);
    ASSERT_TRUE(f1);
    ASSERT_FALSE(f2);
    ASSERT_EQ(2, f0->block_count());
    ASSERT_EQ(2, f1->block_count());

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
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
    test_bootstrap({{1, 2}, {2, 1}});
}

TEST_F(batch_executor_test, variation1) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
    test_bootstrap({{1, 2, 3}, {1}, {1, 3}});
}

TEST_F(batch_executor_test, variation2) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
    test_bootstrap({{100}});
}

TEST_F(batch_executor_test, variation3) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
    test_bootstrap({{1}, {1}, {1}, {1}, {1}});
}

TEST_F(batch_executor_test, max_file_block_params) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
    test_bootstrap({{1, 1, 1}, {1, 1, 1}, {1, 1, 1}}, 1, 1);
}

// temporarily disabled as ci randomly fails
TEST_F(batch_executor_test, DISABLED_files_with_empty_blocks) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
    test_bootstrap({{1, 0, 0}, {0}, {0, 0}, {1}, {0}});
}

TEST_F(batch_executor_test, files_with_empty_blocks_max_params) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
    test_bootstrap({{1, 0, 0}, {0}, {0, 0}, {1}, {0}}, 1, 1);
}

TEST_F(batch_executor_test, all_empty_files) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
    test_bootstrap({{0, 0, 0}, {0}, {0, 0}, {0}, {0}});
}

// TODO failed to file count
TEST_F(batch_executor_test, DISABLED_all_empty_blocks_except_one) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
    test_bootstrap({{1, 0}, {1, 0, 0}, {1, 0, 0}, {1, 0}, {1}, {1}});
}

TEST_F(batch_executor_test, many_files) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
    std::size_t file_count = 100;
    std::vector<std::vector<std::size_t>> defs{};
    defs.reserve(file_count);
    for(std::size_t i=0; i < file_count; ++i) {
        defs.emplace_back(std::vector<std::size_t>{i});
    }
    test_bootstrap(std::move(defs));
}

TEST_F(batch_executor_test, many_files_with_many_empty_ones) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
    std::size_t file_count = 100;
    std::vector<std::vector<std::size_t>> defs{};
    defs.reserve(file_count);
    for(std::size_t i=0; i < file_count/2; ++i) {
        defs.emplace_back(std::vector<std::size_t>{0});
    }
    defs.emplace_back(std::vector<std::size_t>{1});
    for(std::size_t i=0; i < file_count/2; ++i) {
        defs.emplace_back(std::vector<std::size_t>{0});
    }
    test_bootstrap(std::move(defs));
}

TEST_F(batch_executor_test, many_blocks) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
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

// TODO temporarily disable as CI sometimes fails
TEST_F(batch_executor_test, DISABLED_many_blocks_with_many_empty_ones) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
    std::size_t block_count = 100;
    std::vector<std::vector<std::size_t>> defs{};
    std::vector<std::size_t> blocks{};
    blocks.reserve(block_count);

    for(std::size_t i=0; i < block_count/2; ++i) {
        blocks.emplace_back(0);
    }
    blocks.emplace_back(1);
    for(std::size_t i=0; i < block_count/2; ++i) {
        blocks.emplace_back(0);
    }

    defs.emplace_back(std::move(blocks));
    test_bootstrap(std::move(defs));
}


// TODO handle session limit error
TEST_F(batch_executor_test, DISABLED_many_files_and_blocks) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
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

// TODO sometimes failed by err_serialization_failure
TEST_F(batch_executor_test, DISABLED_error_pk_violation) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }
    test_error({{1}, {1}}, status::err_unique_constraint_violation,
        [](std::size_t file_index, std::size_t block_index, std::size_t statement_index, std::size_t& value) {
            value = 0;
        }
    );
}

TEST_F(batch_executor_test, error_on_last_block) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }

    test_error({{1, 1, 1}, {1, 1, 1}}, status::err_unique_constraint_violation,
        [](std::size_t file_index, std::size_t block_index, std::size_t statement_index, std::size_t& value) {
        if(file_index == 1 && block_index == 2) {
            value = 0;
        }
    });
}

// TODO sometimes failed by err_serialization_failure
TEST_F(batch_executor_test, DISABLED_error_on_last_statement) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }

    test_error({{2}, {2}}, status::err_unique_constraint_violation,
        [](std::size_t file_index, std::size_t block_index, std::size_t statement_index, std::size_t& value) {
            if(file_index == 1 && block_index == 0 && statement_index == 1) {
                value = 0;
            }
        }
    );
}

TEST_F(batch_executor_test, error_on_last_statement_of_long_block) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory timed out the testcase";
    }

    test_error({{100}}, status::err_unique_constraint_violation,
        [](std::size_t file_index, std::size_t block_index, std::size_t statement_index, std::size_t& value) {
            if(file_index == 0 && block_index == 0 && statement_index == 99) {
                value = 0;
            }
        }
    );
}

void batch_executor_test::test_bootstrap(
    std::vector<std::vector<std::size_t>> block_def_list,
    std::size_t max_concurrent_files,
    std::size_t max_concurrent_blocks_per_file
) {
    execute_statement("CREATE TABLE TT (C0 BIGINT NOT NULL PRIMARY KEY)");

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
    auto root = batch_executor::create_batch_executor(
        files,
        batch_execution_info{
            prepared,
            std::shared_ptr{std::move(ps)},
            reinterpret_cast<api::impl::database*>(db_.get()),
            [&](){
                called = true;
            },
            batch_executor_option{
                max_concurrent_files,
                max_concurrent_blocks_per_file,
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

void batch_executor_test::test_error(
    std::vector<std::vector<std::size_t>> block_def_list,
    status expected,
    std::function<void(std::size_t, std::size_t, std::size_t, std::size_t&)> customize_value,
    std::size_t max_concurrent_files,
    std::size_t max_concurrent_blocks_per_file
) {
    execute_statement("CREATE TABLE TT (C0 BIGINT NOT NULL PRIMARY KEY)");

    std::size_t file_count = block_def_list.size();
    boost::filesystem::path d{path()};
    std::vector<std::string> files{};
    std::size_t statement_count = 0;
    std::size_t block_count = 0;
    for(std::size_t i=0; i < file_count; ++i) {
        auto file = d / ("simple"+std::to_string(i)+".parquet");
        create_test_file(file, block_def_list[i], statement_count,
            [&, i](std::size_t block_index, std::size_t statement_index, std::size_t& value) {
                if (customize_value) {
                    customize_value(i, block_index, statement_index, value);
                }
            }
        );
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
    auto root = batch_executor::create_batch_executor(
        files,
        batch_execution_info{
            prepared,
            std::shared_ptr{std::move(ps)},
            reinterpret_cast<api::impl::database*>(db_.get()),
            [&](){
                called = true;
            },
            batch_executor_option{
                max_concurrent_files,
                max_concurrent_blocks_per_file,
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
        // just to check manually how long execution proceeded
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TT ORDER BY C0", result);
    }
    EXPECT_TRUE(called);
    auto st = root->state()->status_code();
    auto err_info = root->state()->error_info();
    EXPECT_EQ(expected, st);
    std::cerr << "msg: " << (err_info ? err_info->message() : "") << std::endl;
}
}

