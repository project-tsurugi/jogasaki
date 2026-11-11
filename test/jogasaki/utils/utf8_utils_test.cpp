/*
 * Copyright 2026 Project Tsurugi.
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

#include <jogasaki/utils/utf8_utils.h>

namespace jogasaki::utils {

using namespace std::string_view_literals;

TEST(utf8_utils_test, truncated_sequence_at_nonzero_offset) {
    constexpr auto prefix = "abc"sv;
    for (auto character : {"\xC2\xA2"sv, "\xE3\x81\x82"sv, "\xF0\x9F\x98\x80"sv}) {
        auto input = std::string{prefix} + std::string{character};
        EXPECT_EQ(character.size(), get_byte(detect_next_encoding(input, prefix.size())));
        EXPECT_TRUE(is_valid_utf8(input));
        for (std::size_t remaining = 1; remaining < character.size(); ++remaining) {
            SCOPED_TRACE(character.size());
            SCOPED_TRACE(remaining);
            auto truncated = std::string_view{input}.substr(0, prefix.size() + remaining);
            EXPECT_EQ(encoding_type::INVALID, detect_next_encoding(truncated, prefix.size()));
            EXPECT_FALSE(is_valid_utf8(truncated));
        }
    }
}

} // namespace jogasaki::utils
