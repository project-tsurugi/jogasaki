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

#include <data/variable_legnth_data_region.h>

#include <gtest/gtest.h>
#include <mock_memory_resource.h>

namespace dc::testing {

using namespace data;
using namespace takatori::util;
using namespace std::string_view_literals;

using namespace dc::memory;
using namespace boost::container::pmr;

class variable_length_region_test : public ::testing::Test {
public:
};

TEST_F(variable_length_region_test, basic) {
    mock_memory_resource memory{};
    variable_length_data_region r{&memory, 8};
    r.append("A"sv);
    r.append("AB"sv);
    r.append("ABC"sv);
    ASSERT_EQ(3, r.size());
}

}

