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

#include <jogasaki/data/aligned_buffer.h>

#include <gtest/gtest.h>

#include <jogasaki/accessor/record_ref.h>

#include <jogasaki/mock_memory_resource.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils/record.h>

namespace jogasaki::data {

using namespace testing;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class aligned_buffer_test : public test_root {};

TEST_F(aligned_buffer_test, basic) {
    aligned_buffer buf{10};
    EXPECT_EQ(10, buf.size());
}

}

