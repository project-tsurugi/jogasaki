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
#include <memory>
#include <string>
#include <type_traits>
#include <gtest/gtest.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/io/record_reader.h>

namespace jogasaki::executor {

using namespace std::string_literals;
using namespace std::string_view_literals;

class reader_container_test : public ::testing::Test {};

class test_record_reader : public io::record_reader {
    bool available() const override { return true; };
    bool next_record() override { return true; };
    accessor::record_ref get_record() const override { return {}; }
    void release() override {}
    bool source_active() const noexcept override { return false; }
};

class test_group_reader : public io::group_reader {
    bool next_group() override { return true; };
    accessor::record_ref get_group() const override { return {}; }
    bool next_member() override { return true; }
    accessor::record_ref get_member() const override { return {}; }
    void release() override {}
};

using kind = io::reader_kind;

TEST_F(reader_container_test, simple) {
    test_record_reader rr{};
    test_group_reader gr{};
    {
        io::reader_container c{&rr};
        EXPECT_EQ(kind::record, c.kind());
        auto* r = c.reader<io::record_reader>();
        static_assert(std::is_same_v<io::record_reader*, decltype(r)>);
        EXPECT_TRUE(c);
    }
    {
        io::reader_container c{&gr};
        EXPECT_EQ(kind::group, c.kind());
        auto* r = c.reader<io::group_reader>();
        static_assert(std::is_same_v<io::group_reader*, decltype(r)>);
        EXPECT_TRUE(c);
    }
}

TEST_F(reader_container_test, empty_container) {
    {
        io::reader_container c{};
        EXPECT_FALSE(c);
    }
    {
        io::reader_container c{static_cast<io::record_reader*>(nullptr)};
        EXPECT_FALSE(c);
    }
    {
        io::reader_container c{static_cast<io::group_reader*>(nullptr)};
        EXPECT_FALSE(c);
    }
}

}

