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
#include <jogasaki/executor/process/mock/group_reader.h>

#include "test_root.h"

namespace jogasaki::executor::process::mock {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;
using namespace exchange::group;

class basic_group_reader_test : public test_root {
public:
};

using kind = field_type_kind;

using namespace jogasaki::mock;

TEST_F(basic_group_reader_test, simple) {
    using key_record = basic_record;
    using value_record = basic_record;
    key_record key1{create_record<kind::int4, kind::int8>(1, 10)};
    value_record value1{create_record<kind::float4, kind::float8>(100.0, 1000.0)};
    value_record value2{create_record<kind::float4, kind::float8>(200.0, 2000.0)};
    key_record key3{create_record<kind::int4, kind::int8>(3, 30)};
    value_record value3{create_record<kind::float4, kind::float8>(300.0, 3000.0)};

    using reader_type = basic_group_reader<key_record, value_record>;
    reader_type reader{
        {
            reader_type::group_type{
                key1,
                {
                    value1,
                    value2,
                }
            },
            reader_type::group_type{
                key3,
                {
                    value3,
                }
            },
        }
    };
    auto& k_meta = key1.record_meta();
    auto& v_meta = value1.record_meta();

    ASSERT_TRUE(reader.next_group());
    auto k1 = reader.get_group();
    EXPECT_EQ(basic_record(key1.ref(), k_meta), basic_record(k1, k_meta));
    ASSERT_TRUE(reader.next_member());
    auto v1 = reader.get_member();
    EXPECT_EQ(basic_record(value1.ref(), v_meta), basic_record(v1, v_meta));
    ASSERT_TRUE(reader.next_member());
    auto v2 = reader.get_member();
    EXPECT_EQ(basic_record(value2.ref(), v_meta), basic_record(v2, v_meta));
    ASSERT_FALSE(reader.next_member());
    ASSERT_TRUE(reader.next_group());
    auto k3 = reader.get_group();
    EXPECT_EQ(basic_record(key3.ref(), k_meta), basic_record(k3, k_meta));
    ASSERT_TRUE(reader.next_member());
    auto v3 = reader.get_member();
    EXPECT_EQ(basic_record(value3.ref(), v_meta), basic_record(v3, v_meta));
    ASSERT_FALSE(reader.next_member());
    ASSERT_FALSE(reader.next_group());
}

TEST_F(basic_group_reader_test, meta) {
    // reader type is same, but output metadata is specified
    using input_record = basic_record;

    using key_record = basic_record;
    using value_record = basic_record;

    using key_record = basic_record;
    using value_record = basic_record;
    key_record key1{create_record<kind::int4, kind::int8>(1, 10)};
    value_record value1{create_record<kind::float4, kind::float8>(100.0, 1000.0)};
    value_record value2{create_record<kind::float4, kind::float8>(200.0, 2000.0)};
    key_record key3{create_record<kind::int4, kind::int8>(3, 30)};
    value_record value3{create_record<kind::float4, kind::float8>(300.0, 3000.0)};

    shuffle_info s_info(create_meta<kind::int4, kind::float4, kind::float8, kind::int8>(), {0,3});
    using reader_type = basic_group_reader<key_record, value_record>;
    reader_type reader{
        {
            reader_type::group_type{
                key1,
                {
                    value1,
                    value2,
                }
            },
            reader_type::group_type{
                key3,
                {
                    value3,
                }
            },
        },
        s_info.group_meta()
    };
    auto& k_meta = s_info.key_meta();
    auto& v_meta = s_info.value_meta();

    ASSERT_TRUE(reader.next_group());
    auto k1 = reader.get_group();
    EXPECT_EQ(basic_record(key1.ref(), key1.record_meta()), basic_record(k1, k_meta));
    ASSERT_TRUE(reader.next_member());
    auto v1 = reader.get_member();
    EXPECT_EQ(basic_record(value1.ref(), value1.record_meta()), basic_record(v1, v_meta));
    ASSERT_TRUE(reader.next_member());
    auto v2 = reader.get_member();
    EXPECT_EQ(basic_record(value2.ref(), value2.record_meta()), basic_record(v2, v_meta));
    ASSERT_FALSE(reader.next_member());
    ASSERT_TRUE(reader.next_group());
    auto k3 = reader.get_group();
    EXPECT_EQ(basic_record(key3.ref(), key3.record_meta()), basic_record(k3, k_meta));
    ASSERT_TRUE(reader.next_member());
    auto v3 = reader.get_member();
    EXPECT_EQ(basic_record(value3.ref() ,value3.record_meta()), basic_record(v3, v_meta));
    ASSERT_FALSE(reader.next_member());
    ASSERT_FALSE(reader.next_group());
}

}

