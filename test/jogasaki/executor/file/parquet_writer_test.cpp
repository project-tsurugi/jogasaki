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
#include <jogasaki/executor/file/parquet_writer.h>

#include <gtest/gtest.h>
#include <jogasaki/test_utils/temporary_folder.h>

#include <jogasaki/mock/basic_record.h>

namespace jogasaki::executor::file {

using kind = meta::field_type_kind;
using accessor::text;

class parquet_writer_test : public ::testing::Test {
public:
    void SetUp() override {
        temporary_.prepare();
    }
    void TearDown() override {
        temporary_.clean();
    }

    std::string path() {
        return temporary_.path();
    }

    test::temporary_folder temporary_{};  //NOLINT
};

TEST_F(parquet_writer_test, simple) {
    boost::filesystem::path p{path()};
    p = p / "simple.parquet";
    auto rec = mock::create_nullable_record<kind::int8, kind::float8>(10, 100.0);
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1"}
    ), p.string());
    ASSERT_TRUE(writer);

    EXPECT_TRUE(writer->write(rec.ref()));
    EXPECT_TRUE(writer->write(rec.ref()));
    EXPECT_TRUE(writer->write(rec.ref()));
    EXPECT_TRUE(writer->close());
    EXPECT_EQ(p.string(), writer->path());
    EXPECT_EQ(3, writer->write_count());

    ASSERT_LT(0, boost::filesystem::file_size(p));
}

TEST_F(parquet_writer_test, basic_types1) {
    boost::filesystem::path p{path()};
    p = p / "basic_types1.parquet";
    auto rec = mock::create_nullable_record<kind::int4, kind::int8, kind::float4, kind::float8, kind::character>(1, 10, 100.0, 1000.0, accessor::text("10000"));
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1", "C2", "C3", "C4"}
        ), p.string());
    ASSERT_TRUE(writer);

    EXPECT_TRUE(writer->write(rec.ref()));
    EXPECT_TRUE(writer->write(rec.ref()));
    EXPECT_TRUE(writer->write(rec.ref()));
    EXPECT_TRUE(writer->close());

    ASSERT_LT(0, boost::filesystem::file_size(p));
}

TEST_F(parquet_writer_test, wrong_path) {
    // directory already exists on the specified path
    boost::filesystem::path p{path()};
    auto rec = mock::create_nullable_record<kind::int8, kind::float8>(10, 100.0);
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1"}
        ), p.string());
    ASSERT_FALSE(writer);
}

// depending on environment, permission error doesn't occur
TEST_F(parquet_writer_test, DISABLED_wrong_path2) {
    // no permission to write
    boost::filesystem::path p{"/dummy.parquet"};
    auto rec = mock::create_nullable_record<kind::int8, kind::float8>(10, 100.0);
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1"}
        ), p.string());
    ASSERT_FALSE(writer);
}
}
