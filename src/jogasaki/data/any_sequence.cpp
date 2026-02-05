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
#include "any_sequence.h"

#include <memory>
#include <utility>

#include <jogasaki/error/error_info.h>

namespace jogasaki::data {

any_sequence::any_sequence(any_sequence const& other) = default;

any_sequence::any_sequence(any_sequence&& other) noexcept = default;

any_sequence& any_sequence::operator=(any_sequence const& other) = default;

any_sequence& any_sequence::operator=(any_sequence&& other) noexcept = default;

any_sequence::any_sequence(view_type view) noexcept :
    storage_(view.begin(), view.end())
{}

any_sequence::any_sequence(storage_type values) noexcept :
    storage_(std::move(values))
{}

any_sequence::any_sequence(std::initializer_list<value_type> values) :
    storage_(values)
{}

any_sequence::size_type any_sequence::size() const noexcept {
    return view().size();
}

bool any_sequence::empty() const noexcept {
    return view().empty();
}

any_sequence::value_type const& any_sequence::operator[](size_type index) const noexcept {
    return view()[index];
}

any_sequence::view_type any_sequence::view() const noexcept {
    return view_type{storage_};
}

any_sequence::iterator any_sequence::begin() const noexcept {
    return view().begin();
}

any_sequence::iterator any_sequence::end() const noexcept {
    return view().end();
}

void any_sequence::clear() noexcept {
    storage_.clear();
    error_.reset();
}

void any_sequence::assign(view_type view) noexcept {
    storage_.assign(view.begin(), view.end());
}

void any_sequence::assign(storage_type values) noexcept {
    storage_ = std::move(values);
}

std::shared_ptr<jogasaki::error::error_info> const& any_sequence::error() const noexcept {
    return error_;
}

void any_sequence::error(std::shared_ptr<jogasaki::error::error_info> err) noexcept {
    error_ = std::move(err);
}

bool operator==(any_sequence const& a, any_sequence const& b) noexcept {
    if (a.size() != b.size()) {
        return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (a[i] != b[i]) {
            return false;
        }
    }
    return true;
}

bool operator!=(any_sequence const& a, any_sequence const& b) noexcept {
    return ! (a == b);
}

std::ostream& operator<<(std::ostream& out, any_sequence const& value) {
    out << "any_sequence[";
    bool first = true;
    for (auto const& elem : value) {
        if (! first) {
            out << ", ";
        }
        first = false;
        out << elem;
    }
    out << "]";
    return out;
}

}  // namespace jogasaki::data
