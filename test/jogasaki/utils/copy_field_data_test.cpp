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
#include <jogasaki/utils/copy_field_data.h>

#include <jogasaki/basic_record.h>

#include <gtest/gtest.h>

namespace jogasaki::utils {

using namespace testing;

class copy_field_data_test : public ::testing::Test {};

TEST_F(copy_field_data_test, simple) {

    basic_record<kind::float4, kind::int8> src{1.0, 100};
    basic_record<kind::int8, kind::float4> tgt{200, 2.0};
    basic_record<kind::int8, kind::float4> exp{100, 1.0};
    auto src_meta = src.record_meta();
    auto tgt_meta = tgt.record_meta();
    auto cnt = src_meta->field_count();

    for(std::size_t i=0; i < cnt; ++i) {
        auto j = cnt - 1 - i;
        auto& f = src_meta->at(i);
        auto src_offset = src_meta->value_offset(i);
        auto tgt_offset = tgt_meta->value_offset(j);
        copy_field(f, tgt.ref(), tgt_offset, src.ref(), src_offset);
    }
    ASSERT_EQ(exp, tgt);
}

}

