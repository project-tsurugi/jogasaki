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
#pragma once



#include <optional>
#include <string>
#include <string_view>

#include <jogasaki/dist/remote_execution_block.h>

namespace jogasaki::dist {

// Public result fields intentionally remain aggregate-accessible alongside the success query.
// NOLINTBEGIN(misc-non-private-member-variables-in-classes)
struct remote_execution_block_decode_result {
    std::optional<remote_execution_block> block{};
    std::string error{};
    bool unsupported_type{};

    [[nodiscard]] explicit operator bool() const noexcept { return block.has_value(); }
};
// NOLINTEND(misc-non-private-member-variables-in-classes)

[[nodiscard]] std::optional<std::string> validate_remote_execution_block(remote_execution_block const& block);

[[nodiscard]] remote_execution_block_decode_result decode_remote_execution_block(std::string_view payload);

} // namespace jogasaki::dist
