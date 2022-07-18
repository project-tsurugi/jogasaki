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

#include <jogasaki/kvs/coder.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::index {

/**
 * @brief primary index field info
 * @details mapper uses these fields to know how the key/values on the primary index are mapped to variables
 */
struct cache_align field_info {

    /**
     * @brief create new field information
     * @param type type of the field
     * @param exists whether the target storage exists. If not, there is no room to copy the data to.
     * @param offset byte offset of the target field in the target record reference
     * @param nullity_offset bit offset of the target field nullity in the target record reference
     * @param nullable whether the target field is nullable or not
     * @param spec the spec of the target field used for encode/decode
     */
    field_info(
        meta::field_type type,
        bool exists,
        std::size_t offset,
        std::size_t nullity_offset,
        bool nullable,
        kvs::coding_spec spec
    );

    meta::field_type type_{}; //NOLINT
    bool exists_{}; //NOLINT
    std::size_t offset_{}; //NOLINT
    std::size_t nullity_offset_{}; //NOLINT
    bool nullable_{}; //NOLINT
    kvs::coding_spec spec_{}; //NOLINT
};

}


