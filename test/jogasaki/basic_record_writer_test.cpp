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
#include <jogasaki/executor/process/mock/record_writer.h>

#include "test_root.h"

namespace jogasaki::executor::process::mock {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class basic_record_writer_test : public test_root {};

using kind = field_type_kind;

using namespace jogasaki::mock;

TEST_F(basic_record_writer_test, simple) {
    basic_record rec1{create_record<kind::int4, kind::int8, kind::float4, kind::float8>(1, 10, 100.0, 1000.0)};
    basic_record rec2{create_record<kind::int4, kind::int8, kind::float4, kind::float8>(2, 20, 200.0, 2000.0)};

    basic_record_writer writer{create_writer<kind::int4, kind::int8, kind::float4, kind::float8>()};
    writer.write(rec1.ref());
    writer.write(rec2.ref());
    auto result = writer.records();
    ASSERT_EQ(2, result.size());
    auto meta = result[0].record_meta();
    ASSERT_EQ(*meta, *rec1.record_meta());  // only field types are equal
    EXPECT_EQ(rec1, result[0]);
    EXPECT_EQ(rec2, result[1]);
}

TEST_F(basic_record_writer_test, given_meta) {
    basic_record rec{create_record<kind::int4, kind::int8, kind::float4, kind::float8>(1, 10, 100.0, 1000.0)};
    basic_record_writer writer{rec.record_meta()};

    writer.write(rec.ref());
    auto result = writer.records();
    ASSERT_EQ(1, result.size());
    auto meta = result[0].record_meta();
    ASSERT_EQ(*meta, *rec.record_meta());  // only field types are equal
    EXPECT_EQ(rec, result[0]);
}

TEST_F(basic_record_writer_test, capacity) {
    basic_record rec1{create_record<kind::int4>(1)};
    basic_record rec2{create_record<kind::int4>(2)};
    basic_record rec3{create_record<kind::int4>(3)};
    basic_record rec4{create_record<kind::int4>(4)};

    basic_record_writer writer{create_meta<kind::int4>(), 3};
    writer.write(rec1.ref());
    writer.write(rec2.ref());
    writer.write(rec3.ref());
    writer.write(rec4.ref());
    auto result = writer.records();
    ASSERT_EQ(3, result.size());
    auto meta = result[0].record_meta();
    EXPECT_EQ(rec4, result[0]);
    EXPECT_EQ(rec2, result[1]);
    EXPECT_EQ(rec3, result[2]);
}

}

