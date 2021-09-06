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
#pragma once

#include <gtest/gtest.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_utils/record.h>

namespace jogasaki {

using takatori::util::maybe_shared_ptr;

class test_root : public ::testing::Test {
public:

    /**
     * @brief providing typical record metadata
     */
    static inline maybe_shared_ptr<meta::record_meta> test_record_meta1() {
        test::nullable_record r1{};
        return r1.record_meta();
    }

    static inline maybe_shared_ptr<meta::group_meta> test_group_meta1() {
        return std::make_shared<meta::group_meta>(
                std::make_shared<meta::record_meta>(
                        std::vector<meta::field_type>{
                                meta::field_type(meta::field_enum_tag<meta::field_type_kind::int8>),
                        },
                        boost::dynamic_bitset<std::uint64_t>{std::string("0")}),
                std::make_shared<meta::record_meta>(
                        std::vector<meta::field_type>{
                                meta::field_type(meta::field_enum_tag<meta::field_type_kind::float8>),
                        },
                        boost::dynamic_bitset<std::uint64_t>{std::string("0")})
        );
    }
    static inline maybe_shared_ptr<meta::group_meta> test_group_meta1_kv_reversed() {
        return std::make_shared<meta::group_meta>(
                std::make_shared<meta::record_meta>(
                        std::vector<meta::field_type>{
                                meta::field_type(meta::field_enum_tag<meta::field_type_kind::float8>),
                        },
                        boost::dynamic_bitset<std::uint64_t>{std::string("0")}),
                std::make_shared<meta::record_meta>(
                        std::vector<meta::field_type>{
                                meta::field_type(meta::field_enum_tag<meta::field_type_kind::int8>),
                        },
                        boost::dynamic_bitset<std::uint64_t>{std::string("0")})
        );
    }
};

}
