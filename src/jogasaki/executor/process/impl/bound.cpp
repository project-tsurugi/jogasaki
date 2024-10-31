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
#include "bound.h"

namespace jogasaki::executor::process::impl {

[[nodiscard]] std::string_view bound::key() const noexcept {
    return {static_cast<char*>(key_->data()), len_};
}
[[nodiscard]] kvs::end_point_kind bound::endpointkind() const noexcept { return endpointkind_; }
void bound::dump(std::ostream& out, int indent) const noexcept {
    std::string indent_space(indent, ' ');
    out << indent_space << "bound:"
        << "\n";
    out << indent_space << "  endpointkind_: " << endpointkind_ << "\n";
    out << indent_space << "  len_: " << len_ << "\n";
    out << indent_space << "  key_: " << *key_ << "\n";
    key_->dump(out, indent + 2);
}
} // namespace jogasaki::executor::process::impl