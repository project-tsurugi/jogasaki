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
#include "record_copier.h"

namespace jogasaki::accessor {

record_copier::record_copier(std::shared_ptr<meta::record_meta> meta, memory::paged_memory_resource *resource) :
        meta_(std::move(meta)), resource_(resource) {
    for(std::size_t i=0, n = meta_->field_count(); i < n; ++i) {
        if (meta_->at(i).kind() == meta::field_type_kind::character) {
            text_field_offsets_.emplace_back(meta_->value_offset(i));
        }
    }
}

void record_copier::operator()(void *dst, std::size_t size, accessor::record_ref src) {
    std::memcpy(dst, src.data(), size);
    if (resource_ != nullptr) {
        for(auto& i : text_field_offsets_) {
            auto t = src.get_value<accessor::text>(i);
            auto sv = static_cast<std::string_view>(t);
            text copied{resource_, sv.data(), sv.size()};
            std::memcpy(static_cast<unsigned char*>(dst)+i, &copied, sizeof(text)); //NOLINT
        }
    }
}

void record_copier::operator()(accessor::record_ref dst, accessor::record_ref src) {
    operator()(dst.data(), meta_->record_size(), src);
}
}
