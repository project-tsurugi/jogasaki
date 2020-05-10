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
#pragma once

#include <cstddef>
#include <cstring>

#include <meta/record_meta.h>
#include <accessor/record_ref.h>

namespace jogasaki::accessor {

/**
 * @brief record copy utility
 */
class record_copier {
public:

    using offset_type = record_ref::offset_type;
    using field_index_type = std::size_t;

    /**
     * @brief construct "undefined" object
     */
    constexpr record_copier() = default;

    /**
     * @brief construct object from record metadata
     * @param meta record metadata
     * @param resource memory resource used to copy memory resource based data item such as text
     * (nullptr can be passed if this copier never copies such data item)
     */
    explicit record_copier(std::shared_ptr<meta::record_meta> meta, memory::paged_memory_resource* resource = nullptr) :
            meta_(std::move(meta)), resource_(resource) {
        for(std::size_t i=0, n = meta_->field_count(); i < n; ++i) {
            if (meta_->at(i).kind() == meta::field_type_kind::character) {
                text_field_offsets_.emplace_back(meta_->value_offset(i));
            }
        }
    }

    void copy(accessor::record_ref src, void* dst, std::size_t size) {
        std::memcpy(dst, src.data(), size);
        for(auto& i : text_field_offsets_) {
            auto t = src.get_value<accessor::text>(i);
            auto sv = static_cast<std::string_view>(t);
            text copied{resource_, sv.data(), sv.size()};
            std::memcpy(static_cast<unsigned char*>(dst)+i, &copied, sizeof(text));
        }
    }

    void copy(accessor::record_ref src, accessor::record_ref dst) {
        copy(src, dst.data(), meta_->record_size());
    }

private:
    std::shared_ptr<meta::record_meta> meta_{};
    memory::paged_memory_resource* resource_{};
    std::vector<offset_type> text_field_offsets_{};
};

}
