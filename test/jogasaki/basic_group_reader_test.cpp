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

bool eq(accessor::record_ref a, accessor::record_ref b, record_meta const& meta) {
    std::stringstream ss_a{};
    std::stringstream ss_b{};
    ss_a << a << meta;
    ss_b << b << meta;
    return ss_a.str() == ss_b.str();
}

bool eq(accessor::record_ref a, accessor::record_ref b, record_meta const& meta_a, record_meta const& meta_b) {
    std::stringstream ss_a{};
    std::stringstream ss_b{};
    ss_a << a << meta_a;
    ss_b << b << meta_b;
    return ss_a.str() == ss_b.str();
}

struct record_and_meta {
    record_and_meta(accessor::record_ref record, meta::record_meta const& meta)
    : record_(record), meta_(std::addressof(meta)) {}
    accessor::record_ref record_{};
    meta::record_meta const* meta_;
    friend std::ostream& operator<<(std::ostream& out, record_and_meta const& value) {
        out << value.record_ << *value.meta_;
        return out;
    }
};

using pair = record_and_meta;

inline bool operator==(record_and_meta const& a, record_and_meta const& b) noexcept {
    return eq(a.record_, b.record_, *a.meta_, *b.meta_);
}
inline bool operator!=(record_and_meta const& a, record_and_meta const& b) noexcept {
    return !(a == b);
}

class basic_group_reader_test : public test_root {
public:
};

using kind = field_type_kind;

using namespace jogasaki::mock;

TEST_F(basic_group_reader_test, simple) {
    using key_record = basic_record<kind::int4, kind::int8>;
    using value_record = basic_record<kind::float4, kind::float8>;
    key_record key1{1, 10};
    value_record value1{100.0, 1000.0};
    value_record value2{200.0, 2000.0};
    key_record key3{3, 30};
    value_record value3{300.0, 3000.0};

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
    EXPECT_EQ(pair(key1.ref(), *k_meta), pair(k1, *k_meta));
    ASSERT_TRUE(reader.next_member());
    auto v1 = reader.get_member();
    EXPECT_EQ(pair(value1.ref(), *v_meta), pair(v1, *v_meta));
    ASSERT_TRUE(reader.next_member());
    auto v2 = reader.get_member();
    EXPECT_EQ(pair(value2.ref(), *v_meta), pair(v2, *v_meta));
    ASSERT_FALSE(reader.next_member());
    ASSERT_TRUE(reader.next_group());
    auto k3 = reader.get_group();
    EXPECT_EQ(pair(key3.ref(), *k_meta), pair(k3, *k_meta));
    ASSERT_TRUE(reader.next_member());
    auto v3 = reader.get_member();
    EXPECT_EQ(pair(value3.ref(), *v_meta), pair(v3, *v_meta));
    ASSERT_FALSE(reader.next_member());
    ASSERT_FALSE(reader.next_group());
}

TEST_F(basic_group_reader_test, meta) {
    // reader type is same, but output metadata is specified
    using input_record = basic_record<kind::int4, kind::float4, kind::float8, kind::int8>;

    using key_record = basic_record<kind::int4, kind::int8>;
    using value_record = basic_record<kind::float4, kind::float8>;

    key_record key1{1, 10};
    value_record value1{100.0, 1000.0};
    value_record value2{200.0, 2000.0};
    key_record key3{3, 30};
    value_record value3{300.0, 3000.0};

    shuffle_info s_info(input_record{}.record_meta(), {0,3});
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
    EXPECT_EQ(pair(key1.ref(), *key1.record_meta()), pair(k1, *k_meta));
    ASSERT_TRUE(reader.next_member());
    auto v1 = reader.get_member();
    EXPECT_EQ(pair(value1.ref(), *value1.record_meta()), pair(v1, *v_meta));
    ASSERT_TRUE(reader.next_member());
    auto v2 = reader.get_member();
    EXPECT_EQ(pair(value2.ref(), *value2.record_meta()), pair(v2, *v_meta));
    ASSERT_FALSE(reader.next_member());
    ASSERT_TRUE(reader.next_group());
    auto k3 = reader.get_group();
    EXPECT_EQ(pair(key3.ref(), *key3.record_meta()), pair(k3, *k_meta));
    ASSERT_TRUE(reader.next_member());
    auto v3 = reader.get_member();
    EXPECT_EQ(pair(value3.ref() ,*value3.record_meta()), pair(v3, *v_meta));
    ASSERT_FALSE(reader.next_member());
    ASSERT_FALSE(reader.next_group());
}

}

