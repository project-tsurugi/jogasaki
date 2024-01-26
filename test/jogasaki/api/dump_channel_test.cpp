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

#include <arrow/ipc/api.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/data/any.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/file/arrow_reader.h>
#include <jogasaki/executor/file/parquet_reader.h>
#include <jogasaki/executor/io/dump_channel.h>
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

class dump_channel_test :
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
        std::string_view path,
        std::string_view table_name,
        executor::io::dump_config const& opts,
        std::vector<std::string>& result_files
    ) {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("select * from "+std::string{table_name}, stmt));

        auto transaction = utils::create_transaction(*db_);
        auto tx = get_impl(*db_).find_transaction(*transaction);
        status s{};
        std::string message{"message"};
        std::atomic_bool run{false};
        test_channel ch{};
        ASSERT_TRUE(executor::execute_dump(
            get_impl(*db_),
            tx,
            maybe_shared_ptr{stmt.get()},
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
        if(! message.empty()) {
            std::cerr << "error msg: " << message << std::endl;
        }
        ASSERT_FALSE(ch.writers_.empty());
        auto& wrt = ch.writers_[0];
        auto m = create_file_meta();
        auto recs = deserialize_msg({wrt->data_.data(), wrt->size_}, *m->origin());
        for(auto&& x : recs) {
            LOG(INFO) << x;
            auto name = x.get_value<accessor::text>(0);
            result_files.emplace_back(static_cast<std::string_view>(name));
        }
        EXPECT_TRUE(ch.all_writers_released());
        ASSERT_EQ(status::ok, s);
        ASSERT_EQ(status::ok, executor::commit(get_impl(*db_), tx));
    }
};

using namespace std::string_view_literals;

bool ends_with(std::string_view str, std::string_view suffix) {
    std::size_t cnt = 0;
    for(auto rit = suffix.crbegin(), rit2 = str.crbegin(); rit != suffix.crend() && rit2 != str.crend(); ++rit, ++rit2) {
        if(*rit != *rit2) {
            return false;
        }
        ++cnt;
    }
    return cnt == suffix.size();
}

TEST_F(dump_channel_test, simple_parquet) {
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    for(size_t i=0; i < 10; ++i) {
        execute_statement( "INSERT INTO T VALUES (" + std::to_string(i)+", "+std::to_string(i*10)+")");
    }
    executor::io::dump_config opts{};
    std::vector<std::string> files{};
    test_dump(path(), "T", opts, files);
    ASSERT_EQ(1, files.size());
    EXPECT_TRUE(ends_with(files[0], ".parquet"));
}

std::int64_t get_record_batch_size(arrow::RecordBatch& batch) {
    std::int64_t sz{};
    if(auto res = arrow::ipc::GetRecordBatchSize(batch, &sz); ! res.ok()) {
        LOG(ERROR) << "error retrieving record batch size";
    }
    return sz;
}

std::size_t read_all_records(executor::file::arrow_reader& reader) {
    accessor::record_ref rec{};
    std::size_t cnt = 0;
    while(reader.next(rec)) {
        ++cnt;
    }
    return cnt;
}

TEST_F(dump_channel_test, simple_arrow) {
    // 10 recs written to one file with a single record batch
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    for(size_t i=0; i < 10; ++i) {
        execute_statement( "INSERT INTO T VALUES (" + std::to_string(i)+", "+std::to_string(i*10)+")");
    }
    executor::io::dump_config opts{};
    opts.file_format_ = executor::io::dump_file_format_kind::arrow;
    std::vector<std::string> files{};
    test_dump(path(), "T", opts, files);
    ASSERT_EQ(1, files.size());
    EXPECT_TRUE(ends_with(files[0], ".arrow"));

    auto reader = executor::file::arrow_reader::open(files[0], nullptr, 0);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->row_group_count());
    EXPECT_LT(0, get_record_batch_size(*reader->record_batch()));
    EXPECT_EQ(10, read_all_records(*reader));
}

TEST_F(dump_channel_test, arrow_max_records_per_file) {
    // verify 10 records are split to 2 per file
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    for(size_t i=0; i < 10; ++i) {
        execute_statement( "INSERT INTO T VALUES (" + std::to_string(i)+", "+std::to_string(i*10)+")");
    }
    executor::io::dump_config opts{};
    opts.max_records_per_file_ = 2;
    opts.file_format_ = executor::io::dump_file_format_kind::arrow;
    std::vector<std::string> files{};
    test_dump(path(), "T", opts, files);
    ASSERT_EQ(5, files.size());
    EXPECT_TRUE(ends_with(files[0], ".arrow"));

    auto reader = executor::file::arrow_reader::open(files[0], nullptr, 0);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->row_group_count());
    EXPECT_LT(0, get_record_batch_size(*reader->record_batch()));
    EXPECT_EQ(2, read_all_records(*reader));
}

TEST_F(dump_channel_test, arrow_max_records_per_row_group) {
    // arrow 10 records are split to 5 row group in a single file
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    for(size_t i=0; i < 10; ++i) {
        execute_statement( "INSERT INTO T VALUES (" + std::to_string(i)+", "+std::to_string(i*10)+")");
    }
    executor::io::dump_config opts{};
    opts.record_batch_size_ = 2;
    opts.file_format_ = executor::io::dump_file_format_kind::arrow;
    std::vector<std::string> files{};
    test_dump(path(), "T", opts, files);
    ASSERT_EQ(1, files.size());
    EXPECT_TRUE(ends_with(files[0], ".arrow"));

    auto reader = executor::file::arrow_reader::open(files[0], nullptr, 0);
    ASSERT_TRUE(reader);
    ASSERT_EQ(5, reader->row_group_count());
    EXPECT_LT(0, get_record_batch_size(*reader->record_batch()));
    EXPECT_EQ(2, read_all_records(*reader));
}

TEST_F(dump_channel_test, arrow_max_record_batches_per_file) {
    // arrow 48 records are split to 3 files, 16 row groups for each file, 1 rec for each row group
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    for(size_t i=0; i < 48; ++i) {
        execute_statement( "INSERT INTO T VALUES (" + std::to_string(i)+", "+std::to_string(i*10)+")");
    }
    executor::io::dump_config opts{};
    opts.record_batch_size_ = 1;
    opts.file_format_ = executor::io::dump_file_format_kind::arrow;
    std::vector<std::string> files{};
    test_dump(path(), "T", opts, files);
    ASSERT_EQ(3, files.size());
    EXPECT_TRUE(ends_with(files[0], ".arrow"));

    auto reader0 = executor::file::arrow_reader::open(files[0], nullptr, 0);
    ASSERT_TRUE(reader0);
    ASSERT_EQ(16, reader0->row_group_count());
    EXPECT_LT(0, get_record_batch_size(*reader0->record_batch()));
    EXPECT_EQ(1, read_all_records(*reader0));

    auto reader2 = executor::file::arrow_reader::open(files[2], nullptr, 0);
    ASSERT_TRUE(reader2);
    ASSERT_EQ(16, reader2->row_group_count());
    EXPECT_LT(0, get_record_batch_size(*reader2->record_batch()));
    EXPECT_EQ(1, read_all_records(*reader2));
}

TEST_F(dump_channel_test, arrow_both_max_per_file_and_per_rg) {
    // verify the behavior both limits for file and row group are set
    // arrow 6 records are split to 2 files, each has 2 row groups, first has 2 recs and second 1 rec
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    for(size_t i=0; i < 6; ++i) {
        execute_statement( "INSERT INTO T VALUES (" + std::to_string(i)+", "+std::to_string(i*10)+")");
    }
    executor::io::dump_config opts{};
    opts.record_batch_size_ = 2;
    opts.max_records_per_file_ = 3;
    opts.file_format_ = executor::io::dump_file_format_kind::arrow;
    std::vector<std::string> files{};
    test_dump(path(), "T", opts, files);
    ASSERT_EQ(2, files.size());
    EXPECT_TRUE(ends_with(files[0], ".arrow"));

    auto reader00 = executor::file::arrow_reader::open(files[0], nullptr, 0);
    ASSERT_TRUE(reader00);
    ASSERT_EQ(2, reader00->row_group_count());
    EXPECT_LT(0, get_record_batch_size(*reader00->record_batch()));
    EXPECT_EQ(2, read_all_records(*reader00));

    auto reader01 = executor::file::arrow_reader::open(files[0], nullptr, 1);
    ASSERT_TRUE(reader01);
    ASSERT_EQ(2, reader01->row_group_count());
    EXPECT_LT(0, get_record_batch_size(*reader01->record_batch()));
    EXPECT_EQ(1, read_all_records(*reader01));

    auto reader10 = executor::file::arrow_reader::open(files[1], nullptr, 0);
    ASSERT_TRUE(reader10);
    ASSERT_EQ(2, reader10->row_group_count());
    EXPECT_LT(0, get_record_batch_size(*reader10->record_batch()));
    EXPECT_EQ(2, read_all_records(*reader10));

    auto reader11 = executor::file::arrow_reader::open(files[1], nullptr, 1);
    ASSERT_TRUE(reader11);
    ASSERT_EQ(2, reader11->row_group_count());
    EXPECT_LT(0, get_record_batch_size(*reader11->record_batch()));
    EXPECT_EQ(1, read_all_records(*reader11));
}

TEST_F(dump_channel_test, arrow_char_option) {
    // verify correct type when dumping char(n) data
    // TODO currently char(n) always becomes fixed size binary - fix when char option works correctly
    // TODO currently reader cannot distinguish FIXED_SIZE_BINARY, so manually checking the server log
    //   column name:C0 type:fixed_size_binary[3]
    execute_statement("CREATE TABLE T(C0 CHAR(3) NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES ('000')");

    for(std::size_t loop = 0; loop < 2; ++loop) {
        executor::io::dump_config opts{};
        opts.file_format_ = executor::io::dump_file_format_kind::arrow;
        opts.arrow_use_fixed_size_binary_for_char_ = loop == 0;
        std::vector<std::string> files{};
        test_dump(path(), "T", opts, files);
        ASSERT_EQ(1, files.size());
        EXPECT_TRUE(ends_with(files[0], ".arrow"));

        auto reader = executor::file::arrow_reader::open(files[0], nullptr, 0);
        ASSERT_TRUE(reader);
        auto m = reader->meta();
        auto f = m->at(0);
        ASSERT_EQ(meta::field_type_kind::character, f.kind());

        auto& o = f.option<meta::field_type_kind::character>();
        ASSERT_FALSE(o->varying_);
        ASSERT_EQ(3, o->length_);
    }
}

}  // namespace jogasaki::api
