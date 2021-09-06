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
#include "index_field_mapper.h"

#include <jogasaki/kvs/readable_stream.h>

namespace jogasaki::executor::process::impl::ops {

details::secondary_index_field_info::secondary_index_field_info(
    meta::field_type type,
    bool source_nullable,
    kvs::coding_spec spec
) :
    type_(std::move(type)),
    source_nullable_(source_nullable),
    spec_(spec)
{}

index_field_mapper::index_field_mapper(
    bool use_secondary,
    std::vector<details::field_info> primary_key_fields,
    std::vector<details::field_info> primary_value_fields,
    std::vector<details::secondary_index_field_info> secondary_key_fields
) :
    use_secondary_(use_secondary),
    primary_key_fields_(std::move(primary_key_fields)),
    primary_value_fields_(std::move(primary_value_fields)),
    secondary_key_fields_(std::move(secondary_key_fields))
{}

index_field_mapper::index_field_mapper(
    std::vector<details::field_info> primary_key_fields,
    std::vector<details::field_info> primary_value_fields,
    std::vector<details::secondary_index_field_info> secondary_key_fields
) :
    index_field_mapper(
        true,
        std::move(primary_key_fields),
        std::move(primary_value_fields),
        std::move(secondary_key_fields)
    )
{}

index_field_mapper::index_field_mapper(
    std::vector<details::field_info> primary_key_fields,
    std::vector<details::field_info> primary_value_fields
) :
    index_field_mapper(
        false,
        std::move(primary_key_fields),
        std::move(primary_value_fields),
        {}
    )
{}

status index_field_mapper::operator()(
    std::string_view key,
    std::string_view value,
    accessor::record_ref target,
    kvs::storage& stg,
    kvs::transaction& tx,
    index_field_mapper::memory_resource* resource
) {
    std::string_view k{key};
    std::string_view v{value};
    if (use_secondary_) {
        k = extract_primary_key(key);
        if (auto res = find_primary_index(k, stg, tx, v); res != status::ok) {
            return res;
        }
    }
    populate_field_variables(k, v, target, resource);
    return status::ok;
}

void index_field_mapper::consume_secondary_key_fields(
    std::vector<details::secondary_index_field_info> const& fields,
    kvs::readable_stream& stream
) {
    for(auto&& f : fields) {
        if (f.source_nullable_) {
            kvs::consume_stream_nullable(stream, f.type_, f.spec_);
            continue;
        }
        kvs::consume_stream(stream, f.type_, f.spec_);
    }
}

void index_field_mapper::decode_fields(std::vector<details::field_info> const& fields,
    kvs::readable_stream& stream,
    accessor::record_ref target,
    index_field_mapper::memory_resource* resource
) {
    for(auto&& f : fields) {
        if (! f.exists_) {
            if (f.nullable_) {
                kvs::consume_stream_nullable(stream, f.type_, f.spec_);
                continue;
            }
            kvs::consume_stream(stream, f.type_, f.spec_);
            continue;
        }
        if (f.nullable_) {
            kvs::decode_nullable(
                stream,
                f.type_,
                f.spec_,
                target,
                f.offset_,
                f.nullity_offset_,
                resource
            );
            continue;
        }
        kvs::decode(stream, f.type_, f.spec_, target, f.offset_, resource);
        target.set_null(f.nullity_offset_, false); // currently assuming target variable fields are
        // nullable and f.nullity_offset_ is valid
        // even if f.nullable_ is false
    }
}

std::string_view index_field_mapper::extract_primary_key(std::string_view key) {
    kvs::readable_stream keys{key.data(), key.size()};
    // consume key fields, then the rest is primary key
    consume_secondary_key_fields(secondary_key_fields_, keys);
    return keys.rest();
}

status index_field_mapper::find_primary_index(
    std::string_view key,
    kvs::storage& stg,
    kvs::transaction& tx,
    std::string_view& value_out
) {
    std::string_view v{};
    if(auto res = stg.get(tx, key, v); res != status::ok) {
        if (res == status::not_found) {
            // primary key not found. Inconsistency between primary/secondary indices.
            res = status::err_inconsistent_index;
        }
        return res;
    }
    value_out = v;
    return status::ok;
}

void index_field_mapper::populate_field_variables(
    std::string_view key,
    std::string_view value,
    accessor::record_ref target,
    index_field_mapper::memory_resource* resource
) {
    kvs::readable_stream keys{key.data(), key.size()};
    kvs::readable_stream values{value.data(), value.size()};
    decode_fields(primary_key_fields_, keys, target, resource);
    decode_fields(primary_value_fields_, values, target, resource);
}

}


