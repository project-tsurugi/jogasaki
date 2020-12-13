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

#include <jogasaki/executor/exchange/aggregate/aggregate_info.h>
#include <jogasaki/executor/function/aggregate_function_info.h>
#include <jogasaki/executor/function/builtin_functions.h>

namespace jogasaki::executor::exchange::aggregate {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace executor;
using namespace takatori::util;

class aggregate_info_test : public ::testing::Test {};

using kind = field_type_kind;

TEST_F(aggregate_info_test, simple) {
    auto rec_meta = std::make_shared<record_meta>(std::vector<field_type>{
            field_type(enum_tag<kind::int4>),
            field_type(enum_tag<kind::int8>),
    },boost::dynamic_bitset<std::uint64_t>(2).flip());

    auto func_sum = std::make_shared<function::aggregate_function_info_impl<function::aggregate_function_kind::sum>>();
    aggregate_info info{
        rec_meta,
        {1},
        {
            {
                *func_sum,
                {
                    0
                },
                meta::field_type(enum_tag<kind::int4>)
            }
        },
    };
    EXPECT_EQ(2, info.pre().group_meta()->key_shared()->field_count()); // internal pointer field is added
    EXPECT_EQ(1, info.pre().group_meta()->value_shared()->field_count());
    EXPECT_EQ(2, info.mid().group_meta()->key_shared()->field_count()); // internal pointer field is added
    EXPECT_EQ(1, info.mid().group_meta()->value_shared()->field_count());
    EXPECT_EQ(1, info.post().group_meta()->key_shared()->field_count());
    EXPECT_EQ(1, info.post().group_meta()->value_shared()->field_count());
}

TEST_F(aggregate_info_test, avg_avg) {
    auto rec_meta = std::make_shared<record_meta>(std::vector<field_type>{
        field_type(enum_tag<kind::int4>),
        field_type(enum_tag<kind::int8>),
    },boost::dynamic_bitset<std::uint64_t>(2).flip());

    auto func_avg = std::make_shared<function::aggregate_function_info_impl<function::aggregate_function_kind::avg>>();
    aggregate_info info{
        rec_meta,
        {1},
        {
            {
                *func_avg,
                {
                    0
                },
                meta::field_type(enum_tag<kind::int4>)
            },
            {
                *func_avg,
                    {
                        0
                    },
                    meta::field_type(enum_tag<kind::int4>)
            }
        },
    };
    EXPECT_EQ(2, info.pre().group_meta()->key_shared()->field_count()); // internal pointer field is added
    EXPECT_EQ(4, info.pre().group_meta()->value_shared()->field_count());
    EXPECT_EQ(2, info.mid().group_meta()->key_shared()->field_count()); // internal pointer field is added
    EXPECT_EQ(4, info.mid().group_meta()->value_shared()->field_count());
    EXPECT_EQ(1, info.post().group_meta()->key_shared()->field_count());
    EXPECT_EQ(2, info.post().group_meta()->value_shared()->field_count());

    meta::field_type i4{enum_tag<kind::int4>};
    meta::field_type i8{enum_tag<kind::int8>};
    EXPECT_EQ(i4, info.pre().group_meta()->value_shared()->at(0));
    EXPECT_EQ(i8, info.pre().group_meta()->value_shared()->at(1));
    EXPECT_EQ(i4, info.pre().group_meta()->value_shared()->at(2));
    EXPECT_EQ(i8, info.pre().group_meta()->value_shared()->at(3));
    EXPECT_EQ(i4, info.mid().group_meta()->value_shared()->at(0));
    EXPECT_EQ(i8, info.mid().group_meta()->value_shared()->at(1));
    EXPECT_EQ(i4, info.mid().group_meta()->value_shared()->at(2));
    EXPECT_EQ(i8, info.mid().group_meta()->value_shared()->at(3));
    EXPECT_EQ(i4, info.post().group_meta()->value_shared()->at(0));
    EXPECT_EQ(i4, info.post().group_meta()->value_shared()->at(1));

}

TEST_F(aggregate_info_test, count_avg) {
    auto rec_meta = std::make_shared<record_meta>(std::vector<field_type>{
        field_type(enum_tag<kind::int4>),
        field_type(enum_tag<kind::int8>),
    },boost::dynamic_bitset<std::uint64_t>(2).flip());

    auto func_count = std::make_shared<function::aggregate_function_info_impl<function::aggregate_function_kind::count>>();
    auto func_avg = std::make_shared<function::aggregate_function_info_impl<function::aggregate_function_kind::avg>>();
    aggregate_info info{
        rec_meta,
        {1},
        {
            {
                *func_count,
                {
                    0
                },
                meta::field_type(enum_tag<kind::int8>)
            },
            {
                *func_avg,
                {
                    0
                },
                meta::field_type(enum_tag<kind::int4>)
            }
        },
    };
    EXPECT_EQ(2, info.pre().group_meta()->key_shared()->field_count()); // internal pointer field is added
    EXPECT_EQ(3, info.pre().group_meta()->value_shared()->field_count());
    EXPECT_EQ(2, info.mid().group_meta()->key_shared()->field_count()); // internal pointer field is added
    EXPECT_EQ(3, info.mid().group_meta()->value_shared()->field_count());
    EXPECT_EQ(1, info.post().group_meta()->key_shared()->field_count());
    EXPECT_EQ(2, info.post().group_meta()->value_shared()->field_count());

    meta::field_type i4{enum_tag<kind::int4>};
    meta::field_type i8{enum_tag<kind::int8>};
    EXPECT_EQ(i8, info.pre().group_meta()->value_shared()->at(0));
    EXPECT_EQ(i4, info.pre().group_meta()->value_shared()->at(1));
    EXPECT_EQ(i8, info.pre().group_meta()->value_shared()->at(2));

    EXPECT_EQ(i8, info.mid().group_meta()->value_shared()->at(0));
    EXPECT_EQ(i4, info.mid().group_meta()->value_shared()->at(1));
    EXPECT_EQ(i8, info.mid().group_meta()->value_shared()->at(2));

    EXPECT_EQ(i8, info.post().group_meta()->value_shared()->at(0));
    EXPECT_EQ(i4, info.post().group_meta()->value_shared()->at(1));

}

}

