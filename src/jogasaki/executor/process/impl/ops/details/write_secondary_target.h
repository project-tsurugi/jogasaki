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

#include <vector>

#include <yugawara/storage/index.h>
#include <yugawara/binding/factory.h>
#include <takatori/relation/write.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/exception.h>

#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/kvs/coder.h>
#include "write_secondary_context.h"

namespace jogasaki::executor::process::impl::ops::details {

using takatori::util::maybe_shared_ptr;
using takatori::util::throw_exception;

/**
 * @brief field info of the update operation
 * @details update operation uses these fields to know how the variables or input record fields are are mapped to
 * key/value fields. The update operation retrieves the key/value records from kvs and decode to
 * the record (of key/value respectively), updates the record fields by replacing the value with one from variable table
 * record (source), encodes the record and puts into kvs.
 */
struct cache_align secondary_key_field : index::field_info {
    /**
     * @brief create new object
     * @param type type of the field
     * @param offset byte offset of the field in the input variables record (in variable table)
     * @param nullity_offset bit offset of the field nullity in the input variables record
     * @param nullable whether the target field is nullable or not
     * @param key indicates the filed is part of the key
     */
    secondary_key_field(
        meta::field_type type,
        std::size_t offset,
        std::size_t nullity_offset,
        bool nullable,
        kvs::coding_spec spec,
        bool key
    ) :
        field_info(
            std::move(type),
            true,
            offset,
            nullity_offset,
            nullable,
            spec
        ),
        key_(key)
    {}

    bool key_{}; //NOLINT
};
/**
 * @brief secondary target for write
 * @details this object represents write operation interface for secondary index
 * It's associated the following records whose fields info. are given in the constructor.
 * - source key/value records
 *   - the source columns to generate secondary index key
 */
class write_secondary_target {
public:
    friend class write_secondary_context;

    using key = takatori::relation::write::key;
    using column = takatori::relation::write::column;
    using memory_resource = memory::lifo_paged_memory_resource;
    using variable = takatori::descriptor::variable;

    /**
     * @brief create empty object
     */
    write_secondary_target() = default;

    /**
     * @brief create new object
     * @param storage_name the primary storage name to write
     * @param secondary_key_fields the secondary key fields
     */
    write_secondary_target(
        std::string_view storage_name,
        std::vector<details::secondary_key_field> secondary_key_fields
    ) :
        storage_name_(storage_name),
        secondary_key_fields_(std::move(secondary_key_fields))
    {}

    ~write_secondary_target() = default;
    write_secondary_target(write_secondary_target const& other) = default;
    write_secondary_target& operator=(write_secondary_target const& other) = default;
    write_secondary_target(write_secondary_target&& other) noexcept = default;
    write_secondary_target& operator=(write_secondary_target&& other) noexcept = default;

    /**
     * @brief create new object from takatori columns
     * @param idx target index information
     */
    write_secondary_target(
        yugawara::storage::index const& idx,
        maybe_shared_ptr<meta::record_meta> const& primary_key_meta,
        maybe_shared_ptr<meta::record_meta> const& primary_value_meta
    ) :
        write_secondary_target(
            idx.simple_name(),
            create_fields(idx, primary_key_meta, primary_value_meta)
        )
    {}

    /**
     * @brief encode key/value and put them to index
     * @returns status::ok when successful
     * @returns any other error otherwise
     * @note this uses `upsert` so status::already_exist is not expected to be returned
     */
    status encode_and_put(
        write_secondary_context& ctx,
        transaction_context& tx,
        accessor::record_ref source_key,
        accessor::record_ref source_value,
        std::string_view primary_key
    ) const;

    /**
     * @brief encode key and remove the corresponding entry in the index
     * @returns status::ok when successful
     * @returns status::not_found when target entry is not found
     * @returns any other error otherwise
     */
    status encode_and_remove(
        write_secondary_context& ctx,
        transaction_context& tx,
        accessor::record_ref source_key,
        accessor::record_ref source_value,
        std::string_view primary_key
    ) const;

    /**
     * @brief accessor to storage name
     */
    [[nodiscard]] std::string_view storage_name() const noexcept {
        return storage_name_;
    }

private:
    std::string storage_name_{};
    std::vector<details::secondary_key_field> secondary_key_fields_{};

    status encode_key(
        write_secondary_context& ctx,
        accessor::record_ref source_key,
        accessor::record_ref source_value,
        std::string_view primary_key,
        std::string_view& out
    ) const;

    std::vector<details::secondary_key_field> create_fields(
        yugawara::storage::index const& idx,
        maybe_shared_ptr<meta::record_meta> const& primary_key_meta, //NOLINT
        maybe_shared_ptr<meta::record_meta> const& primary_value_meta //NOLINT
    );
};

}