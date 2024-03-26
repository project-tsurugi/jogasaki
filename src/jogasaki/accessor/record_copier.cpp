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
#include "record_copier.h"

#include <cstring>
#include <string_view>
#include <type_traits>
#include <utility>
#include <boost/assert.hpp>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::accessor {

record_copier::record_copier(
    maybe_shared_ptr<meta::record_meta> meta,
    memory::paged_memory_resource *resource
) :
    meta_(std::move(meta)),
    resource_(resource)
{
    for(std::size_t i=0, n = meta_->field_count(); i < n; ++i) {
        if (meta_->at(i).kind() == meta::field_type_kind::character) {
            text_field_offsets_.emplace_back(meta_->value_offset(i));
            text_field_nullity_offsets_.emplace_back(meta_->nullity_offset(i));
            text_field_nullability_.emplace_back(meta_->nullable(i));
        }
    }
}

void record_copier::operator()(void *dst, std::size_t size, accessor::record_ref src) {
    BOOST_ASSERT(size <= src.size());  //NOLINT
    std::memcpy(dst, src.data(), size);
    if (resource_ != nullptr) {
        for(std::size_t i = 0, n = text_field_offsets_.size(); i < n ; ++i) {
            if (text_field_nullability_[i]) {
                auto nullity_offset = text_field_nullity_offsets_[i];
                if(src.is_null(nullity_offset)) continue;
            }
            auto value_offset = text_field_offsets_[i];
            auto t = src.get_value<accessor::text>(value_offset);
            auto sv = static_cast<std::string_view>(t);
            text copied{resource_, sv.data(), sv.size()};
            std::memcpy(static_cast<unsigned char*>(dst)+value_offset, &copied, sizeof(text)); //NOLINT
        }
    }
}

void record_copier::operator()(accessor::record_ref dst, accessor::record_ref src) {
    operator()(dst.data(), meta_->record_size(), src);
}

}
