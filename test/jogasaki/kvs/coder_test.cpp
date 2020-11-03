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
#include <jogasaki/kvs/coder.h>

#include <string>

#include <takatori/util/object_creator.h>
#include <gtest/gtest.h>

#include <jogasaki/test_root.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/environment.h>

#include <jogasaki/mock_memory_resource.h>

namespace jogasaki::kvs {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;
using namespace kvs::details;

class coder_test : public test_root {
public:
    static void SetUpTestCase() {}
    coder_test() {}
private:
};

TEST_F(coder_test, simple) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    int_t<32> i32{1};
    int_t<64> i64{2};
    float_t<32> f32{3};
    float_t<64> f64{4};
    mock_memory_resource resource{};
    accessor::text txt{&resource, "ABC"sv};
    accessor::text txt2{&resource, "ABC"sv};
    s.write(i32, true);
    s.write(f32, true);
    s.write(i64, true);
    s.write(f64, true);
    s.write(txt, true);

    s.reset();

    ASSERT_EQ(i32, s.read<std::int32_t>(true));
    ASSERT_EQ(f32, s.read<float>(true));
    ASSERT_EQ(i64, s.read<std::int64_t>(true));
    ASSERT_EQ(f64, s.read<double>(true));
    ASSERT_EQ(txt2, s.read<accessor::text>(&resource, true));
}

TEST_F(coder_test, descendant) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    int_t<32> i32{1};
    int_t<64> i64{2};
    float_t<32> f32{3};
    float_t<64> f64{4};
    mock_memory_resource resource{};
    accessor::text txt{&resource, "ABC"sv};
    accessor::text txt2{&resource, "ABC"sv};
    s.write(i32, false);
    s.write(f32, false);
    s.write(i64, false);
    s.write(f64, false);
    s.write(txt, false);

    s.reset();

    ASSERT_EQ(i32, s.read<std::int32_t>(false));
    ASSERT_EQ(f32, s.read<float>(false));
    ASSERT_EQ(i64, s.read<std::int64_t>(false));
    ASSERT_EQ(f64, s.read<double>(false));
    ASSERT_EQ(txt, s.read<accessor::text>(&resource, false));
}

TEST_F(coder_test, i32_asc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    int_t<32> i1{2};
    int_t<32> i2{-2};
    s.write(i1, true);
    s.write(i2, true);

    s.reset();

    ASSERT_EQ(i1, s.read<std::int32_t>(true));
    ASSERT_EQ(i2, s.read<std::int32_t>(true));
    EXPECT_EQ('\x80', buf[0]);
    EXPECT_EQ('\x00', buf[1]);
    EXPECT_EQ('\x00', buf[2]);
    EXPECT_EQ('\x02', buf[3]);
    EXPECT_EQ('\x7F', buf[4]);
    EXPECT_EQ('\xFF', buf[5]);
    EXPECT_EQ('\xFF', buf[6]);
    EXPECT_EQ('\xFE', buf[7]);
}

TEST_F(coder_test, i32_desc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    int_t<32> i1{2};
    int_t<32> i2{-2};
    s.write(i1, false);
    s.write(i2, false);

    s.reset();

    ASSERT_EQ(i1, s.read<std::int32_t>(false));
    ASSERT_EQ(i2, s.read<std::int32_t>(false));
    EXPECT_EQ('\x7F', buf[0]);
    EXPECT_EQ('\xFF', buf[1]);
    EXPECT_EQ('\xFF', buf[2]);
    EXPECT_EQ('\xFD', buf[3]);
    EXPECT_EQ('\x80', buf[4]);
    EXPECT_EQ('\x00', buf[5]);
    EXPECT_EQ('\x00', buf[6]);
    EXPECT_EQ('\x01', buf[7]);
}

TEST_F(coder_test, i64_asc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    int_t<64> i1{2};
    int_t<64> i2{-2};
    s.write(i1, true);
    s.write(i2, true);

    s.reset();

    ASSERT_EQ(i1, s.read<std::int64_t>(true));
    ASSERT_EQ(i2, s.read<std::int64_t>(true));
    EXPECT_EQ('\x80', buf[0]);
    EXPECT_EQ('\x00', buf[1]);
    EXPECT_EQ('\x00', buf[2]);
    EXPECT_EQ('\x00', buf[3]);
    EXPECT_EQ('\x00', buf[4]);
    EXPECT_EQ('\x00', buf[5]);
    EXPECT_EQ('\x00', buf[6]);
    EXPECT_EQ('\x02', buf[7]);

    EXPECT_EQ('\x7F', buf[8]);
    EXPECT_EQ('\xFF', buf[9]);
    EXPECT_EQ('\xFF', buf[10]);
    EXPECT_EQ('\xFF', buf[11]);
    EXPECT_EQ('\xFF', buf[12]);
    EXPECT_EQ('\xFF', buf[13]);
    EXPECT_EQ('\xFF', buf[14]);
    EXPECT_EQ('\xFE', buf[15]);
}

TEST_F(coder_test, f32_asc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    float_t<32> f1{2};
    float_t<32> f2{-2};
    s.write(f1, true);
    s.write(f2, true);

    s.reset();

    ASSERT_EQ(f1, s.read<float>(true));
    ASSERT_EQ(f2, s.read<float>(true));
    EXPECT_EQ('\xC0', buf[0]);
    EXPECT_EQ('\x00', buf[1]);
    EXPECT_EQ('\x00', buf[2]);
    EXPECT_EQ('\x00', buf[3]);
    EXPECT_EQ('\x3F', buf[4]);
    EXPECT_EQ('\xFF', buf[5]);
    EXPECT_EQ('\xFF', buf[6]);
    EXPECT_EQ('\xFF', buf[7]);
}

TEST_F(coder_test, f32_desc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    float_t<32> f1{2};
    float_t<32> f2{-2};
    s.write(f1, false);
    s.write(f2, false);

    s.reset();

    ASSERT_EQ(f1, s.read<float>(false));
    ASSERT_EQ(f2, s.read<float>(false));
    EXPECT_EQ('\x3F', buf[0]);
    EXPECT_EQ('\xFF', buf[1]);
    EXPECT_EQ('\xFF', buf[2]);
    EXPECT_EQ('\xFF', buf[3]);
    EXPECT_EQ('\xC0', buf[4]);
    EXPECT_EQ('\x00', buf[5]);
    EXPECT_EQ('\x00', buf[6]);
    EXPECT_EQ('\x00', buf[7]);
}

TEST_F(coder_test, float_nan) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    float_t<32> f1{std::nanf("1")};
    float_t<32> f2{std::nanf("2")};
    float_t<64> f3{std::nan("3")};
    float_t<64> f4{std::nan("4")};
    s.write(f1, true);
    s.write(f2, false);
    s.write(f3, true);
    s.write(f4, false);

    s.reset();

    ASSERT_TRUE(std::isnan(s.read<float>(true)));
    ASSERT_TRUE(std::isnan(s.read<float>(false)));
    ASSERT_TRUE(std::isnan(s.read<double>(true)));
    ASSERT_TRUE(std::isnan(s.read<double>(false)));
}

TEST_F(coder_test, f64_asc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    float_t<64> f1{2};
    float_t<64> f2{-2};
    s.write(f1, true);
    s.write(f2, true);

    s.reset();

    ASSERT_EQ(f1, s.read<double>(true));
    ASSERT_EQ(f2, s.read<double>(true));
    EXPECT_EQ('\xC0', buf[0]);
    EXPECT_EQ('\x00', buf[1]);
    EXPECT_EQ('\x00', buf[2]);
    EXPECT_EQ('\x00', buf[3]);
    EXPECT_EQ('\x00', buf[4]);
    EXPECT_EQ('\x00', buf[5]);
    EXPECT_EQ('\x00', buf[6]);
    EXPECT_EQ('\x00', buf[7]);
    EXPECT_EQ('\x3F', buf[8]);
    EXPECT_EQ('\xFF', buf[9]);
    EXPECT_EQ('\xFF', buf[10]);
    EXPECT_EQ('\xFF', buf[11]);
    EXPECT_EQ('\xFF', buf[12]);
    EXPECT_EQ('\xFF', buf[13]);
    EXPECT_EQ('\xFF', buf[14]);
    EXPECT_EQ('\xFF', buf[15]);
}

TEST_F(coder_test, text) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    mock_memory_resource resource{};
    accessor::text txt{&resource, "ABC"sv};
    s.write(txt, true);

    s.reset();

    ASSERT_EQ(txt, s.read<accessor::text>(&resource, true));
    EXPECT_EQ('\x80', buf[0]);
    EXPECT_EQ('\x03', buf[1]);
    EXPECT_EQ('A', buf[2]);
    EXPECT_EQ('B', buf[3]);
    EXPECT_EQ('C', buf[4]);
}

TEST_F(coder_test, empty_text) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    mock_memory_resource resource{};
    accessor::text txt{&resource, ""sv};
    s.write(txt, true);

    s.reset();
    auto result = s.read<accessor::text>(&resource, true);
    ASSERT_EQ(txt, result);
    ASSERT_EQ(0, result.size());
    EXPECT_EQ('\x80', buf[0]);
    EXPECT_EQ('\x00', buf[1]);
}
}

