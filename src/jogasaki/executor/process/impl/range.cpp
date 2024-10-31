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

#include "range.h"

namespace jogasaki::executor::process::impl {

range::range(std::unique_ptr<bound> begin, std::unique_ptr<bound> end, bool is_empty) noexcept
    : begin_(std::move(begin)), end_(std::move(end)), is_empty_(is_empty) {}

[[nodiscard]] bound const* range::begin() const noexcept { return begin_.get(); }
[[nodiscard]] bound const* range::end() const noexcept { return end_.get(); }
[[nodiscard]] bool range::is_empty() const noexcept { return is_empty_; }

void range::dump(std::ostream& out, int indent) const noexcept {
    std::string indent_space(indent, ' ');
    out << indent_space << "  begin_:\n";
    begin_->dump(out, indent + 2);
    out << indent_space << "  end_:\n";
    end_->dump(out, indent + 2);
    out << indent_space << "  is_empty_: " <<  is_empty_ << "\n";
}
} // namespace jogasaki::executor::process::impl