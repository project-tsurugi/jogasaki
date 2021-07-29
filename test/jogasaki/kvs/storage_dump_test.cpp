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
#include <jogasaki/kvs/storage_dump.h>

#include <memory>
#include <gtest/gtest.h>

#include "kvs_test_base.h"

namespace jogasaki::kvs {

using namespace std::literals::string_literals;

class storage_dump_test :
    public ::testing::Test,
    public kvs_test_base {

public:
    void SetUp() override {
        db_setup();
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(storage_dump_test, dump_manual) {
    std::stringstream ss;

    storage_dump::append(ss, "a", "AAA");
    storage_dump::append_eof(ss);
    ss.seekg(0);

    std::string key, value;
    ASSERT_EQ(storage_dump::read_next(ss, key, value), true);
    EXPECT_EQ(key, "a");
    EXPECT_EQ(value, "AAA");

    EXPECT_EQ(storage_dump::read_next(ss, key, value), false);
}

TEST_F(storage_dump_test, dump_manual_empty) {
    std::stringstream ss;

    storage_dump::append_eof(ss);

    std::string key, value;
    EXPECT_EQ(storage_dump::read_next(ss, key, value), false);
}

TEST_F(storage_dump_test, dump_manual_multiple) {
    std::stringstream ss;

    storage_dump::append(ss, "a", "AAA");
    storage_dump::append(ss, "b", "BBB");
    storage_dump::append(ss, "c", "CCC");
    storage_dump::append_eof(ss);
    ss.seekg(0);

    std::string key, value;
    ASSERT_EQ(storage_dump::read_next(ss, key, value), true);
    EXPECT_EQ(key, "a");
    EXPECT_EQ(value, "AAA");

    ASSERT_EQ(storage_dump::read_next(ss, key, value), true);
    EXPECT_EQ(key, "b");
    EXPECT_EQ(value, "BBB");

    ASSERT_EQ(storage_dump::read_next(ss, key, value), true);
    EXPECT_EQ(key, "c");
    EXPECT_EQ(value, "CCC");

    EXPECT_EQ(storage_dump::read_next(ss, key, value), false);
}

TEST_F(storage_dump_test, dump_empty) {
    storage_dump dumper { *db_ };

    std::stringstream ss;

    // FIXME: we cannot detect that the target table exists or empty
    dumper.dump(ss, "temp");
    ss.seekg(0);

    std::string key, value;
    EXPECT_EQ(storage_dump::read_next(ss, key, value), false);
}

TEST_F(storage_dump_test, load_dump) {
    std::stringstream ss;
    storage_dump::append(ss, "a", "AAA");
    storage_dump::append(ss, "b", "BBB");
    storage_dump::append(ss, "c", "CCC");
    storage_dump::append_eof(ss);
    ss.seekg(0);

    storage_dump dumper { *db_ };
    dumper.load(ss, "temp");
    ss.seekp(0);

    dumper.dump(ss, "temp");
    ss.seekg(0);

    std::string key, value;
    ASSERT_EQ(storage_dump::read_next(ss, key, value), true);
    EXPECT_EQ(key, "a");
    EXPECT_EQ(value, "AAA");

    ASSERT_EQ(storage_dump::read_next(ss, key, value), true);
    EXPECT_EQ(key, "b");
    EXPECT_EQ(value, "BBB");

    ASSERT_EQ(storage_dump::read_next(ss, key, value), true);
    EXPECT_EQ(key, "c");
    EXPECT_EQ(value, "CCC");

    EXPECT_EQ(storage_dump::read_next(ss, key, value), false);
}

TEST_F(storage_dump_test, load_dump_batch) {
    std::stringstream ss;
    storage_dump::append(ss, "a", "AAA");
    storage_dump::append(ss, "b", "BBB");
    storage_dump::append(ss, "c", "CCC");
    storage_dump::append_eof(ss);
    ss.seekg(0);

    storage_dump dumper { *db_ };
    dumper.load(ss, "temp", 2);
    ss.seekp(0);

    dumper.dump(ss, "temp", 2);
    ss.seekg(0);

    std::string key, value;
    ASSERT_EQ(storage_dump::read_next(ss, key, value), true);
    EXPECT_EQ(key, "a");
    EXPECT_EQ(value, "AAA");

    ASSERT_EQ(storage_dump::read_next(ss, key, value), true);
    EXPECT_EQ(key, "b");
    EXPECT_EQ(value, "BBB");

    ASSERT_EQ(storage_dump::read_next(ss, key, value), true);
    EXPECT_EQ(key, "c");
    EXPECT_EQ(value, "CCC");

    EXPECT_EQ(storage_dump::read_next(ss, key, value), false);
}

}
