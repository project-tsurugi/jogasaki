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
#include "index_accessor.h"

#include <cstddef>

#include <yugawara/storage/index.h>

#include <jogasaki/index/field_info.h>

namespace jogasaki::index {

status decode_fields(
    std::vector<index::field_info> const& fields,
    kvs::readable_stream& stream,
    accessor::record_ref target,
    memory::lifo_paged_memory_resource* resource
) {
    for(auto&& f : fields) {
        if (! f.exists_) {
            if (f.nullable_) {
                if(auto res = kvs::consume_stream_nullable(stream, f.type_, f.spec_); res != status::ok) {
                    return res;
                }
                continue;
            }
            if(auto res = kvs::consume_stream(stream, f.type_, f.spec_); res != status::ok) {
                return res;
            }
            continue;
        }
        if (f.nullable_) {
            if(auto res = kvs::decode_nullable(
                    stream,
                    f.type_,
                    f.spec_,
                    target,
                    f.offset_,
                    f.nullity_offset_,
                    resource
                ); res != status::ok) {
                return res;
            }
            continue;
        }
        if(auto res = kvs::decode(stream, f.type_, f.spec_, target, f.offset_, resource); res != status::ok) {
            return res;
        }
        target.set_null(f.nullity_offset_, false); // currently assuming target variable fields are
        // nullable and f.nullity_offset_ is valid
        // even if f.nullable_ is false
    }
    return status::ok;
}

mapper::mapper(std::vector<field_info> key_fields, std::vector<field_info> value_fields) :
    key_fields_(std::move(key_fields)),
    value_fields_(std::move(value_fields))
{}

bool mapper::read(
    bool key,
    kvs::readable_stream& stream,
    accessor::record_ref target,
    memory::lifo_paged_memory_resource* resource
) {
    auto& flds = key ? key_fields_ : value_fields_;
    return decode_fields(flds, stream, target, resource) == status::ok;
}

}  // namespace jogasaki::index
