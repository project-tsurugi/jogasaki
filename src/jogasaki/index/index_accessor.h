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

#include <yugawara/storage/index.h>

#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/index/field_info.h>

namespace jogasaki::index {

status decode_fields(
    std::vector<index::field_info> const& fields,
    kvs::readable_stream& stream,
    accessor::record_ref target,
    memory::lifo_paged_memory_resource* resource
);

class mapper {
public:
    mapper() = default;

    mapper(
        std::vector<field_info> key_fields,
        std::vector<field_info> value_fields
    ) :
        key_fields_(std::move(key_fields)),
        value_fields_(std::move(value_fields))
    {}

    bool read(
        bool key,
        kvs::readable_stream& stream,
        accessor::record_ref target,
        memory::lifo_paged_memory_resource* resource
    ) {
        auto& flds = key ? key_fields_ : value_fields_;
        return decode_fields(flds, stream, target, resource) == status::ok;
    }

    bool write(bool key, accessor::record_ref src, kvs::writable_stream& stream) {
        (void) key;
        (void) stream;
        (void) src;
        return true;
    }
private:
    std::vector<field_info> key_fields_{};
    std::vector<field_info> value_fields_{};
};

}


