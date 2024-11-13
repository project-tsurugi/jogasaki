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

#include <boost/assert.hpp>

#include "scan_range.h"

namespace jogasaki::executor::process::impl {

scan_range::scan_range(bound begin, bound end, bool is_empty) noexcept
    : begin_(std::move(begin)), end_(std::move(end)), is_empty_(is_empty) {}
scan_range::scan_range() noexcept: is_empty_(true) {}
[[nodiscard]] bound const& scan_range::begin() const noexcept { return begin_; }
[[nodiscard]] bound const& scan_range::end() const noexcept { return end_; }
[[nodiscard]] bool scan_range::is_empty() const noexcept { return is_empty_; }

void scan_range::dump(std::ostream& out, int indent) const noexcept {
    std::string indent_space(indent, ' ');
    out << indent_space << "  begin_:\n";
    begin_.dump(out, indent + 2);
    out << indent_space << "  end_:\n";
    end_.dump(out, indent + 2);
    out << indent_space << "  is_empty_: " << is_empty_ << "\n";
}
} // namespace jogasaki::executor::process::impl
