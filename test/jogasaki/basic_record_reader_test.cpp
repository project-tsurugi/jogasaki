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
#include <jogasaki/executor/process/mock/record_reader.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

#include "test_root.h"

namespace jogasaki::executor::process::mock {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class basic_record_reader_test : public test_root {
public:
};

using kind = field_type_kind;

using namespace jogasaki::mock;

TEST_F(basic_record_reader_test, simple) {
    basic_record src1{create_record<kind::int4, kind::int8, kind::float4, kind::float8>(1, 10, 100.0, 1000.0)};
    basic_record src2{create_record<kind::int4, kind::int8, kind::float4, kind::float8>(2, 20, 200.0, 2000.0)};

    basic_record_reader reader{
        {
            src1,
            src2,
        },
        src1.record_meta()
    };
    ASSERT_TRUE(reader.next_record());
    auto rec1 = reader.get_record();
    ASSERT_TRUE(reader.next_record());
    auto rec2 = reader.get_record();
    ASSERT_FALSE(reader.next_record());
    EXPECT_EQ(src1, basic_record(rec1, src1.record_meta()));
    EXPECT_EQ(src2, basic_record(rec2, src2.record_meta()));
}

TEST_F(basic_record_reader_test, given_meta) {
    basic_record src{create_record<kind::int4, kind::int8, kind::float4, kind::float8>(1, 10, 100.0, 1000.0)};
    basic_record_reader reader{
        {
            src,
        },
        src.record_meta()
    };

    ASSERT_TRUE(reader.next_record());
    auto rec = reader.get_record();
    ASSERT_FALSE(reader.next_record());
    EXPECT_EQ(src, basic_record(rec, src.record_meta()));
}

TEST_F(basic_record_reader_test, generate) {
    using reader_type = basic_record_reader;
    basic_record src{create_record<kind::int4>(1)};
    reader_type reader{
        2, reader_type::npos, []() { return create_record<kind::int4>(1); }
    };
    ASSERT_TRUE(reader.next_record());
    auto rec1 = reader.get_record();
    EXPECT_EQ(src, basic_record(rec1, src.record_meta()));
    ASSERT_TRUE(reader.next_record());
    auto rec2 = reader.get_record();
    EXPECT_EQ(src, basic_record(rec2, src.record_meta()));
    ASSERT_FALSE(reader.next_record());
}

TEST_F(basic_record_reader_test, repeats) {
    using reader_type = basic_record_reader;
    basic_record src{create_record<kind::int4>(1)};
    reader_type reader{
        {
            create_record<kind::int4>(1),
        },
        src.record_meta()
    };
    reader.repeats(2);
    ASSERT_TRUE(reader.next_record());
    auto rec1 = reader.get_record();
    EXPECT_EQ(src, basic_record(rec1, src.record_meta()));
    ASSERT_TRUE(reader.next_record());
    auto rec2 = reader.get_record();
    EXPECT_EQ(src, basic_record(rec2, src.record_meta()));
    ASSERT_FALSE(reader.next_record());
}

TEST_F(basic_record_reader_test, use_memory_allocator) {
    basic_record src1{create_record<kind::int4, kind::int8, kind::float4, kind::float8>(1, 10, 100.0, 1000.0)};
    basic_record src2{create_record<kind::int4, kind::int8, kind::float4, kind::float8>(2, 20, 200.0, 2000.0)};
    using records_type = boost::container::pmr::vector<basic_record>;
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};
    records_type records{&resource};
    records.emplace_back(src1);
    records.emplace_back(src2);

    basic_record_reader reader{
        std::move(records),
        src1.record_meta(),
    };
    ASSERT_TRUE(reader.next_record());
    auto rec1 = reader.get_record();
    ASSERT_TRUE(reader.next_record());
    auto rec2 = reader.get_record();
    ASSERT_FALSE(reader.next_record());
    EXPECT_EQ(src1, basic_record(rec1, src1.record_meta()));
    EXPECT_EQ(src2, basic_record(rec2, src2.record_meta()));
}

TEST_F(basic_record_reader_test, generate_records_with_memory_allocator) {
    basic_record src1{create_record<kind::int4, kind::int8, kind::float4, kind::float8>(1, 10, 100.0, 1000.0)};
    basic_record src2{create_record<kind::int4, kind::int8, kind::float4, kind::float8>(2, 20, 200.0, 2000.0)};
    using records_type = boost::container::pmr::vector<basic_record>;
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};

    basic_record_reader reader{
        2,
        std::size_t(-1),
        []() {
            return create_record<kind::int4, kind::int8, kind::float4, kind::float8>(1, 10, 100.0, 1000.0);
        },
        &resource,
        src1.record_meta()
    };
    ASSERT_TRUE(reader.next_record());
    auto rec1 = reader.get_record();
    ASSERT_TRUE(reader.next_record());
    auto rec2 = reader.get_record();
    ASSERT_FALSE(reader.next_record());
    EXPECT_EQ(src1, basic_record(rec1, src1.record_meta()));
    EXPECT_EQ(src1, basic_record(rec2, src2.record_meta()));
}

}

