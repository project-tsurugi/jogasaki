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
#include <jogasaki/meta/group_meta.h>

#include <gtest/gtest.h>
#include <boost/dynamic_bitset.hpp>

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class group_meta_test : public ::testing::Test {};

using kind = field_type_kind;

TEST_F(group_meta_test, single_field) {
    group_meta meta{
            record_meta{
                    std::vector<field_type>{
                            field_type(field_enum_tag<kind::int8>),
                    },
                    boost::dynamic_bitset<std::uint64_t>{"1"s}
            },
            record_meta{
                    std::vector<field_type>{
                            field_type(field_enum_tag<kind::float8>),
                    },
                    boost::dynamic_bitset<std::uint64_t>{"1"s}
            },
    };

    EXPECT_EQ(1, meta.key().field_count());
    EXPECT_EQ(1, meta.value().field_count());
    EXPECT_TRUE(meta.key().nullable(0));
    EXPECT_TRUE(meta.value().nullable(0));
    EXPECT_EQ(field_type(field_enum_tag<kind::int8>), meta.key()[0]);
    EXPECT_EQ(field_type(field_enum_tag<kind::float8>), meta.value()[0]);
}

TEST_F(group_meta_test, equality) {
    group_meta meta10{
            record_meta{
                    std::vector<field_type>{
                            field_type(field_enum_tag<kind::int8>),
                    },
                    boost::dynamic_bitset<std::uint64_t>{"1"s}
            },
            record_meta{
                    std::vector<field_type>{
                            field_type(field_enum_tag<kind::float8>),
                    },
                    boost::dynamic_bitset<std::uint64_t>{"1"s}
            },
    };
    group_meta meta11{
            record_meta{
                    std::vector<field_type>{
                            field_type(field_enum_tag<kind::int8>),
                    },
                    boost::dynamic_bitset<std::uint64_t>{"1"s}
            },
            record_meta{
                    std::vector<field_type>{
                            field_type(field_enum_tag<kind::float8>),
                    },
                    boost::dynamic_bitset<std::uint64_t>{"1"s}
            },
    };
    group_meta meta2{
            record_meta{
                    std::vector<field_type>{
                            field_type(field_enum_tag<kind::int8>),
                            field_type(field_enum_tag<kind::int8>),
                    },
                    boost::dynamic_bitset<std::uint64_t>{"11"s}
            },
            record_meta{
                    std::vector<field_type>{
                            field_type(field_enum_tag<kind::float8>),
                            field_type(field_enum_tag<kind::float8>),
                    },
                    boost::dynamic_bitset<std::uint64_t>{"11"s}
            },
    };

    EXPECT_EQ(meta10, meta11);
    EXPECT_EQ(meta11, meta10);
    EXPECT_NE(meta10, meta2);
}

TEST_F(group_meta_test, access_shared_ptr) {
    group_meta meta{
            record_meta{
                    std::vector<field_type>{
                            field_type(field_enum_tag<kind::int8>),
                    },
                    boost::dynamic_bitset<std::uint64_t>{"1"s}
            },
            record_meta{
                    std::vector<field_type>{
                            field_type(field_enum_tag<kind::float8>),
                    },
                    boost::dynamic_bitset<std::uint64_t>{"1"s}
            },
    };

    auto key = meta.key_shared();
    auto owner = key.ownership();
    EXPECT_EQ(3, owner.use_count());
}

}

