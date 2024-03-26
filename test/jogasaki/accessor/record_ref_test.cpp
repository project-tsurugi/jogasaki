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
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <jogasaki/accessor/record_ref.h>


namespace jogasaki::testing {

using namespace std::string_view_literals;
using namespace accessor;

class record_ref_test : public ::testing::Test {
public:
};

TEST_F(record_ref_test, basic) {
    struct {
        std::int64_t x_;
        std::int64_t y_;
        std::int64_t z_;
    } buffer;
    record_ref r{&buffer, sizeof(buffer)};
    ASSERT_EQ(24, r.size());
    buffer.x_ = 1;
    buffer.y_ = 2;
    buffer.z_ = 3;

    EXPECT_EQ(1, r.get_value<std::int64_t>(0));
    EXPECT_EQ(2, r.get_value<std::int64_t>(8));
    EXPECT_EQ(3, r.get_value<std::int64_t>(16));
}

TEST_F(record_ref_test, nullity) {
    struct {
        std::int64_t x_;
        std::int64_t y_;
        std::int64_t z_;
        char n_[1];
    } buffer;
    record_ref r{&buffer, sizeof(buffer)};
    ASSERT_EQ(32, r.size());
    buffer.x_ = 1;
    buffer.y_ = 2;
    buffer.z_ = 3;

    std::size_t nullity_bit_base = 24*8;
    r.set_null(nullity_bit_base + 0, false);
    r.set_null(nullity_bit_base + 1, true);
    r.set_null(nullity_bit_base + 2, false);

    EXPECT_EQ(1, *r.get_if<std::int64_t>(nullity_bit_base+0, 0));
    EXPECT_FALSE(r.get_if<std::int64_t>(nullity_bit_base+1, 8));
    EXPECT_EQ(3, *r.get_if<std::int64_t>(nullity_bit_base+2, 16));

    EXPECT_FALSE(r.is_null(nullity_bit_base+0));
    EXPECT_TRUE(r.is_null(nullity_bit_base+1));
    EXPECT_FALSE(r.is_null(nullity_bit_base+2));

}



}

