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

#include <gtest/gtest.h>
#include <boost/dynamic_bitset.hpp>
#include <executor/partitioner.h>

namespace dc::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class partitioner_test : public ::testing::Test {};

using kind = field_type_kind;

TEST_F(partitioner_test, simple) {
    auto meta = std::make_shared<record_meta>(
            std::vector<field_type>{
                    field_type(enum_tag<kind::int4>),
                    field_type(enum_tag<kind::int8>),
            },
            boost::dynamic_bitset<std::uint64_t>{"00"s});

    executor::partitioner p{5, std::move(meta)};
    struct {
        std::int32_t x_;
        std::int64_t y_;
    } buffer[3];
    accessor::record_ref r0{&buffer[0], sizeof(buffer[0])};
    accessor::record_ref r1{&buffer[1], sizeof(buffer[1])};
    accessor::record_ref r2{&buffer[2], sizeof(buffer[2])};
    buffer[0].x_=1;
    buffer[0].y_=1000;
    buffer[1].x_=2;
    buffer[1].y_=2000;

    buffer[2].x_=3;
    buffer[2].y_=3000;
    EXPECT_NE(p(r0), p(r1));
    EXPECT_NE(p(r1), p(r2));
    EXPECT_NE(p(r2), p(r0));
}

}

