/*
* Copyright 2018-2023 Project Tsurugi.
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
#include "external_record_meta.h"

#include <cstddef>
#include <type_traits>
#include <utility>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::meta {

using takatori::util::maybe_shared_ptr;

external_record_meta::external_record_meta(maybe_shared_ptr<record_meta> origin,
    std::vector<std::optional<std::string>> field_names) :
    record_meta_(std::move(origin)),
    field_names_(std::move(field_names))
{}

field_type const& external_record_meta::operator[](external_record_meta::field_index_type index) const noexcept {
    return record_meta_->operator[](index);
}

field_type const& external_record_meta::at(external_record_meta::field_index_type index) const noexcept {
    return record_meta_->at(index);
}

external_record_meta::value_offset_type
external_record_meta::value_offset(external_record_meta::field_index_type index) const noexcept {
    return record_meta_->value_offset(index);
}

external_record_meta::nullity_offset_type
external_record_meta::nullity_offset(external_record_meta::field_index_type index) const noexcept {
    return record_meta_->nullity_offset(index);
}

bool external_record_meta::nullable(external_record_meta::field_index_type index) const noexcept {
    return record_meta_->nullable(index);
}

std::size_t external_record_meta::record_alignment() const noexcept {
    return record_meta_->record_alignment();
}

std::size_t external_record_meta::record_size() const noexcept {
    return record_meta_->record_size();
}

std::size_t external_record_meta::field_count() const noexcept {
    return record_meta_->field_count();
}

external_record_meta::field_iterator external_record_meta::begin() const noexcept {
    return record_meta_->begin();
}

external_record_meta::field_iterator external_record_meta::end() const noexcept {
    return record_meta_->end();
}

std::optional<std::string_view>
external_record_meta::field_name(external_record_meta::field_index_type index) const noexcept {
    auto& name = field_names_[index];
    return name ? std::optional<std::string_view>{*name} : std::optional<std::string_view>{};
}

maybe_shared_ptr<record_meta> const& external_record_meta::origin() noexcept {
    return record_meta_;
}

external_record_meta::field_index_type external_record_meta::field_index(std::string_view name) const noexcept {
    for(std::size_t i=0, n=field_count(); i < n; ++i) {
        if(field_names_[i] == name) {
            return i;
        }
    }
    return undefined;
}
} // namespace

