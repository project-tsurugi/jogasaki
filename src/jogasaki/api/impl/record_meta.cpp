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
#include "record_meta.h"

#include <algorithm>
#include <type_traits>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/field_type.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::api::impl {

using takatori::util::maybe_shared_ptr;


record_meta::record_meta(maybe_shared_ptr<meta::record_meta> meta) :
    meta_(std::move(meta))
{
    fields_.reserve(meta_->field_count());
    for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
        fields_.emplace_back(meta_->at(i));
    }
}

field_type const& record_meta::at(record_meta::field_index_type index) const noexcept {
    return fields_[index];
}

bool record_meta::nullable(record_meta::field_index_type index) const noexcept {
    return meta_->nullable(index);
}

std::size_t record_meta::field_count() const noexcept {
    return meta_->field_count();
}

maybe_shared_ptr<meta::record_meta> const& record_meta::meta() const noexcept {
    return meta_;
}
} // namespace

