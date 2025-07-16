/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <cstdint>
#include <memory>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>

#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::meta {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class external_record_meta_test : public ::testing::Test {};

using kind = field_type_kind;

TEST_F(external_record_meta_test, find) {
    meta::external_record_meta meta{

        std::make_shared<record_meta>(
            std::vector<field_type>{
                field_type(field_enum_tag<kind::int8>),
                field_type(field_enum_tag<kind::float8>),
            },
            boost::dynamic_bitset<std::uint64_t>{"11"s}
        ),
        std::vector<std::optional<std::string>>{"C0", "C1"}
    };

    EXPECT_EQ(2, meta.field_count());
    EXPECT_EQ("C0", meta.field_name(0));
    EXPECT_EQ("C1", meta.field_name(1));
    EXPECT_EQ(0, meta.field_index("C0"));
    EXPECT_EQ(1, meta.field_index("C1"));
    EXPECT_EQ(external_record_meta::undefined, meta.field_index("dummy"));
}

}

