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

#include <vector>

#include <takatori/relation/find.h>

#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/kvs/coder.h>
#include "operator_base.h"
#include "find_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using takatori::util::unsafe_downcast;

namespace details {

/**
 * @brief primary index field info
 * @details mapper uses these fields to know how the key/values on the primary index are mapped to variables
 */
struct cache_align field_info {

    /**
     * @brief create new field information
     * @param type type of the field
     * @param target_exists whether the target storage exists. If not, there is no room to copy the data to.
     * @param target_offset byte offset of the target field in the target record reference
     * @param target_nullity_offset bit offset of the target field nullity in the target record reference
     * @param source_nullable whether the target field is nullable or not
     * @param spec the spec of the target field used for encode/decode
     */
    field_info(
        meta::field_type type,
        bool target_exists,
        std::size_t target_offset,
        std::size_t target_nullity_offset,
        bool source_nullable,
        kvs::coding_spec spec
    ) :
        type_(std::move(type)),
        target_exists_(target_exists),
        target_offset_(target_offset),
        target_nullity_offset_(target_nullity_offset),
        source_nullable_(source_nullable),
        spec_(spec)
    {}

    meta::field_type type_{}; //NOLINT
    bool target_exists_{}; //NOLINT
    std::size_t target_offset_{}; //NOLINT
    std::size_t target_nullity_offset_{}; //NOLINT
    bool source_nullable_{}; //NOLINT
    kvs::coding_spec spec_{}; //NOLINT
};

/**
 * @brief secondary index field info
 * @details mapper uses these fields to extract the primary key from secondary key
 */
struct cache_align secondary_index_field_info {

    /**
     * @brief create new field information
     * @param type type of the field
     * @param source_nullable whether the target field is nullable or not
     * @param spec the spec of the target field used for encode/decode
     */
    secondary_index_field_info(
        meta::field_type type,
        bool source_nullable,
        kvs::coding_spec spec
    ) :
        type_(std::move(type)),
        source_nullable_(source_nullable),
        spec_(spec)
    {}

    meta::field_type type_{}; //NOLINT
    bool source_nullable_{}; //NOLINT
    kvs::coding_spec spec_{}; //NOLINT
};
}

/**
 * @brief index fields mapper object
 * @details this object knows the mapping from secondary index (if any) to primary, receives the key/value and fill
 * relation fields by resolving the primary key and identifying field values on the primary index.
 */
class index_field_mapper {
public:
    using column = takatori::relation::find::column;

    using memory_resource = memory::lifo_paged_memory_resource;

    /**
     * @brief create empty object
     */
    index_field_mapper() = default;

    /**
     * @brief create new object
     */
    index_field_mapper(
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

    /**
     * @brief create new object using secondary
     */
    index_field_mapper(
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

    /**
     * @brief create new object without secondary
     */
    index_field_mapper(
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

    status operator()(
        std::string_view key,
        std::string_view value,
        accessor::record_ref target,
        kvs::storage& stg,
        kvs::transaction& tx,
        memory_resource* resource
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

private:
    bool use_secondary_{};
    std::vector<details::field_info> primary_key_fields_{};
    std::vector<details::field_info> primary_value_fields_{};
    std::vector<details::secondary_index_field_info> secondary_key_fields_{};

    void consume_secondary_key_fields(
        std::vector<details::secondary_index_field_info> const& fields,
        kvs::stream& stream
    ) {
        for(auto&& f : fields) {
            if (f.source_nullable_) {
                kvs::consume_stream_nullable(stream, f.type_, f.spec_);
                continue;
            }
            kvs::consume_stream(stream, f.type_, f.spec_);
        }
    }
    void decode_fields(
        std::vector<details::field_info> const& fields,
        kvs::stream& stream,
        accessor::record_ref target,
        memory_resource* resource
    ) {
        for(auto&& f : fields) {
            if (! f.target_exists_) {
                if (f.source_nullable_) {
                    kvs::consume_stream_nullable(stream, f.type_, f.spec_);
                    continue;
                }
                kvs::consume_stream(stream, f.type_, f.spec_);
                continue;
            }
            if (f.source_nullable_) {
                kvs::decode_nullable(
                    stream,
                    f.type_,
                    f.spec_,
                    target,
                    f.target_offset_,
                    f.target_nullity_offset_,
                    resource
                );
                continue;
            }
            kvs::decode(stream, f.type_, f.spec_, target, f.target_offset_, resource);
            target.set_null(f.target_nullity_offset_, false); // currently assuming target variable fields are
            // nullable and f.target_nullity_offset_ is valid
            // even if f.source_nullable_ is false
        }
    }

    std::string_view extract_primary_key(
        std::string_view key
    ) {
        kvs::stream keys{const_cast<char*>(key.data()), key.size()}; //TODO create read-only stream
        // consume key fields, then the rest is primary key
        consume_secondary_key_fields(secondary_key_fields_, keys);
        return keys.rest();
    }

    status find_primary_index(
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

    void populate_field_variables(
        std::string_view key,
        std::string_view value,
        accessor::record_ref target,
        memory_resource* resource
    ) {
        kvs::stream keys{const_cast<char*>(key.data()), key.size()}; //TODO create read-only stream
        kvs::stream values{const_cast<char*>(value.data()), value.size()}; //   and avoid using const_cast
        decode_fields(primary_key_fields_, keys, target, resource);
        decode_fields(primary_value_fields_, values, target, resource);
    }

};


}


