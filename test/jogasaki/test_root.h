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

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki {

class test_root : public ::testing::Test {
public:

    /**
     * @brief providing typical record metadata
     */
    static inline std::shared_ptr<meta::record_meta> test_record_meta1() {
        return std::make_shared<meta::record_meta>(
                std::vector<meta::field_type>{
                        meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int8>),
                        meta::field_type(takatori::util::enum_tag<meta::field_type_kind::float8>),
                },
                boost::dynamic_bitset<std::uint64_t>{std::string("00")});
    }

    static inline std::shared_ptr<meta::record_meta> test_record_meta2() {
        return std::make_shared<meta::record_meta>(
                std::vector<meta::field_type>{
                        meta::field_type(takatori::util::enum_tag<meta::field_type_kind::character>),
                        meta::field_type(takatori::util::enum_tag<meta::field_type_kind::float8>),
                        meta::field_type(takatori::util::enum_tag<meta::field_type_kind::character>),
                },
                boost::dynamic_bitset<std::uint64_t>{std::string("000")});
    }
};

}
