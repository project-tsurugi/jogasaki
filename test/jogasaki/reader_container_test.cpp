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
#include <executor/reader_container.h>

#include <gtest/gtest.h>

namespace jogasaki::executor {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace takatori::util;

class reader_container_test : public ::testing::Test {};

class test_record_reader : public record_reader {
    bool available() const override { return true; };
    bool next_record() override { return true; };
    accessor::record_ref get_record() const override { return {}; }
    void release() override {}
};

class test_group_reader : public group_reader {
    bool next_group() override { return true; };
    accessor::record_ref get_group() const override { return {}; }
    bool next_member() override { return true; }
    accessor::record_ref get_member() const override { return {}; }
    void release() override {}
};

using kind = reader_kind;

TEST_F(reader_container_test, simple) {
    test_record_reader rr{};
    test_group_reader gr{};
    {
        reader_container c{&rr};
        EXPECT_EQ(kind::record, c.kind());
        auto* r = c.reader<record_reader>();
        static_assert(std::is_same_v<record_reader*, decltype(r)>);
    }
    {
        reader_container c{&gr};
        EXPECT_EQ(kind::group, c.kind());
        auto* r = c.reader<group_reader>();
        static_assert(std::is_same_v<group_reader*, decltype(r)>);
    }
}

}

