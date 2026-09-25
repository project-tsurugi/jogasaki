/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/mock_memory_resource.h>

namespace jogasaki::testing {

using namespace std::string_view_literals;

TEST(binary_test, copy_preserves_long_value_after_source_changes) {
    mock_memory_resource resource{};
    std::string source{"A234567890123456"};

    auto value = accessor::binary::copy(resource, source);
    source.assign(source.size(), 'X');

    EXPECT_EQ("A234567890123456"sv, static_cast<std::string_view>(value));
    EXPECT_EQ(16, resource.total_bytes_allocated_);
}

}  // namespace jogasaki::testing
