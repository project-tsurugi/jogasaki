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
#include <jogasaki/mock/basic_record.h>

#include "test_root.h"

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class basic_record_test : public test_root {};

using kind = field_type_kind;

using namespace jogasaki::mock;

TEST_F(basic_record_test, simple) {
    basic_record r{create_record<kind::int4>(2)};

    test::record rec{1, 100.0};
    EXPECT_EQ(1, rec.key());
    EXPECT_EQ(100.0, rec.value());
}

TEST_F(basic_record_test, meta) {
    {
        basic_record r{create_record<kind::int4>()};
        auto meta = r.record_meta();
        EXPECT_EQ(1, meta->field_count());
        EXPECT_EQ(meta::field_type{takatori::util::enum_tag<kind::int4>}, meta->at(0));
    }
    {
        basic_record r{create_record<kind::int4, kind::int8>()};
        auto meta = r.record_meta();
        EXPECT_EQ(2, meta->field_count());
        EXPECT_EQ(meta::field_type{takatori::util::enum_tag<kind::int4>}, meta->at(0));
        EXPECT_EQ(meta::field_type{takatori::util::enum_tag<kind::int8>}, meta->at(1));
    }
}

TEST_F(basic_record_test, default_metadata) {
    basic_record r{create_record<kind::float4, kind::int8>(1.0, 100)};
    basic_record r2{r.ref(), create_meta<kind::float4, kind::int8>()};
    auto meta = r2.record_meta();

    EXPECT_EQ(1.0, r2.ref().get_value<float>(meta->value_offset(0)));
    EXPECT_EQ(100, r2.ref().get_value<std::int64_t>(meta->value_offset(1)));
}

TEST_F(basic_record_test, share_metadata) {
    basic_record r{create_record<kind::int4>(1)};
    auto meta = r.record_meta();
    basic_record r2{create_record<kind::int4>(meta, 2)};
    auto meta2 = r2.record_meta();
    EXPECT_EQ(1, meta2->field_count());
    EXPECT_EQ(meta::field_type{takatori::util::enum_tag<kind::int4>}, meta2->at(0));
    EXPECT_EQ(meta, meta2);
}

}
