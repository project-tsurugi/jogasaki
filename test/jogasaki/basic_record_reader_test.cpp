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
#include <jogasaki/executor/process/mock/record_reader.h>

#include "test_root.h"

namespace jogasaki::executor::process::mock {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class basic_record_reader_test : public test_root {
public:
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
};

using kind = field_type_kind;

using namespace jogasaki::mock;

TEST_F(basic_record_reader_test, simple) {
    using test_record = basic_record<kind::int4, kind::int8, kind::float4, kind::float8>;
    test_record src1{1, 10, 100.0, 1000.0};
    test_record src2{2, 20, 200.0, 2000.0};

    basic_record_reader<test_record> reader{
        {
            src1,
            src2,
        }
    };
    ASSERT_TRUE(reader.next_record());
    auto rec1 = reader.get_record();
    ASSERT_TRUE(reader.next_record());
    auto rec2 = reader.get_record();
    ASSERT_FALSE(reader.next_record());
    EXPECT_TRUE(eq(src1.ref(), rec1, *src1.record_meta()));
    EXPECT_TRUE(eq(src2.ref(), rec2, *src2.record_meta()));
}

TEST_F(basic_record_reader_test, given_meta) {
    using test_record = basic_record<kind::int4, kind::int8, kind::float4, kind::float8>;
    test_record src{1, 10, 100.0, 1000.0};
    basic_record_reader<test_record> reader{
        {
            src,
        },
        src.record_meta()
    };

    ASSERT_TRUE(reader.next_record());
    auto rec = reader.get_record();
    ASSERT_FALSE(reader.next_record());
    EXPECT_TRUE(eq(src.ref(), rec, *src.record_meta()));
}

TEST_F(basic_record_reader_test, given_meta_with_map) {
    using test_record = basic_record<kind::int4, kind::int8, kind::float4, kind::float8>;
    using reversed_record = basic_record<kind::float8, kind::float4, kind::int8, kind::int4>;
    test_record src{1, 10, 100.0, 1000.0};
    reversed_record rev{1000, 100.0, 10, 1};

    basic_record_reader<test_record> reader{
        {
            src,
        },
        rev.record_meta(),
        {
            {0, 3},
            {1, 2},
            {2, 1},
            {3, 0},
        }
    };

    ASSERT_TRUE(reader.next_record());
    auto rec = reader.get_record();
    ASSERT_FALSE(reader.next_record());
    EXPECT_TRUE(eq(rev.ref(), rec, *rev.record_meta()));
}

TEST_F(basic_record_reader_test, generate) {
    using test_record = basic_record<kind::int4>;

    using reader_type = basic_record_reader<test_record>;
    test_record src{1};
    reader_type reader{
        2, reader_type::npos, []() { return test_record{1}; }
    };
    ASSERT_TRUE(reader.next_record());
    auto rec1 = reader.get_record();
    EXPECT_TRUE(eq(src.ref(), rec1, *src.record_meta()));
    ASSERT_TRUE(reader.next_record());
    auto rec2 = reader.get_record();
    EXPECT_TRUE(eq(src.ref(), rec2, *src.record_meta()));
    ASSERT_FALSE(reader.next_record());
}

TEST_F(basic_record_reader_test, repeats) {
    using test_record = basic_record<kind::int4>;
    using reader_type = basic_record_reader<test_record>;
    test_record src{1};
    reader_type reader{
        {
            test_record{1},
        },
    };
    reader.repeats(2);
    ASSERT_TRUE(reader.next_record());
    auto rec1 = reader.get_record();
    EXPECT_TRUE(eq(src.ref(), rec1, *src.record_meta()));
    ASSERT_TRUE(reader.next_record());
    auto rec2 = reader.get_record();
    EXPECT_TRUE(eq(src.ref(), rec2, *src.record_meta()));
    ASSERT_FALSE(reader.next_record());
}

}

