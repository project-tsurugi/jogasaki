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
#pragma once

#include <cstddef>
#include <cstring>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/accessor/record_ref.h>

namespace jogasaki::accessor {

using takatori::util::maybe_shared_ptr;

/**
 * @brief record copy utility
 * @details this copier copies the record content to target record region considering memory resource dependent data
 * item such as accessor::text. Both deep/shallow copy on such data items are supported.
 */
class record_copier {
public:

    /// @brief type of field offset
    using offset_type = record_ref::offset_type;

    /// @brief type of field index
    using field_index_type = std::size_t;

    /**
     * @brief construct "undefined" object
     */
    constexpr record_copier() = default;

    /**
     * @brief construct object from record metadata
     * @param meta record metadata
     * @param resource memory resource to copy memory resource dependent data item (e.g. `text` field type data).
     * Pass nullptr if this copier never copies such data item, or shallow copy is sufficient (i.e. only reference to
     * memory resource is copied)
     */
    explicit record_copier(maybe_shared_ptr<meta::record_meta> meta, memory::paged_memory_resource* resource = nullptr);

    /**
     * @brief copy record content referenced by record_ref
     * @param dst copy destination
     * @param size size of the record content
     * @param src copy source
     */
    void operator()(void* dst, std::size_t size, accessor::record_ref src);

    /**
     * @brief copy record content referenced by record_ref
     * @param dst copy destination
     * @param src copy source
     */
    void operator()(accessor::record_ref dst, accessor::record_ref src);

private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    memory::paged_memory_resource* resource_{};
    std::vector<offset_type> text_field_offsets_{};
    std::vector<offset_type> text_field_nullity_offsets_{};
    std::vector<bool> text_field_nullability_{};
};

}
