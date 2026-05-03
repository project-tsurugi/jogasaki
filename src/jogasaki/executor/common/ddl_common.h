/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <string>
#include <string_view>

#include <yugawara/storage/index.h>
#include <yugawara/storage/sequence.h>

#include <jogasaki/auth/action_set.h>
#include <jogasaki/auth/authorized_users_action_set.h>
#include <jogasaki/request_context.h>
#include <jogasaki/storage/storage_list.h>

namespace jogasaki::executor::common {

/**
 * @brief Create a new sequence in the system table and assign a definition_id to @p p.
 * @param context request context containing the transaction and sequence manager
 * @param p sequence object whose definition_id will be populated
 * @param new_def_id if true, assign a new definition_id to the sequence;
 * if false, use the existing definition_id in @p p
 * @return true on success; false with context error set on failure
 */
bool create_generated_sequence(request_context& context, yugawara::storage::sequence& p, bool new_def_id = true);

/**
 * @brief Remove a sequence from the system table (metadata_store).
 * If the sequence has no definition_id the function returns true immediately.
 * @param context request context containing the transaction and sequence manager
 * @param sequence_name name used in log messages
 * @param s sequence whose definition_id entry is removed from the system table
 * @return true on success; false with context error set on unrecoverable KVS error
 */
bool remove_generated_sequence(
    request_context& context,
    std::string_view sequence_name,
    yugawara::storage::sequence const& s
);

/**
 * @brief Mark the KVS storage backing @p index as delete-reserved in its metadata.
 * @param context request context
 * @param index_name logical name of the index (used for key lookup and logging)
 * @param index index whose stored metadata is updated
 * @return true on success; false with context error set on failure
 */
bool reserve_delete_index_metadata(
    request_context& context,
    std::string_view index_name,
    yugawara::storage::index const& index
);

/**
 * @brief Acquire a write lock on a newly created storage entry within the current DDL transaction.
 * @param context request context containing the transaction
 * @param tid storage entry to lock
 * @param table_name table name used in error messages
 * @return true on success; false with context error set when lock is blocked
 */
bool lock_storage_entry(
    request_context& context,
    storage::storage_entry tid,
    std::string_view table_name
);

/**
 * @brief Create a new primary index storage entry in the storage manager and the corresponding KVS storage.
 * Provider metadata is NOT updated — the caller is responsible for updating it if needed.
 * @param context request context
 * @param primary_idx primary index whose metadata is serialized into the new storage
 * @param authorized_actions if non-null, assigned to the new entry's authorized_actions
 * @param public_actions if non-null, assigned to the new entry's public_actions
 * @param [out] tid receives the new storage entry id on success
 * @param [out] serialized if non-null, receives a copy of the serialized storage option
 * @return true on success; false with context error set on failure
 */
bool create_primary_storage(
    request_context& context,
    yugawara::storage::index const& primary_idx,
    auth::authorized_users_action_set const* authorized_actions,
    auth::action_set const* public_actions,
    storage::storage_entry& tid,
    std::string* serialized = nullptr
);

/**
 * @brief Create a new secondary index storage entry in the storage manager and the corresponding KVS storage.
 * Provider metadata is NOT updated — the caller is responsible for updating it if needed.
 * @param context request context
 * @param sec_idx secondary index whose metadata is serialized into the new storage
 * @param primary_entry storage entry of the owning primary index
 * @param [out] tid receives the new storage entry id on success
 * @param [out] serialized if non-null, receives a copy of the serialized storage option
 * @return true on success; false with context error set on failure
 */
bool create_secondary_storage(
    request_context& context,
    yugawara::storage::index const& sec_idx,
    storage::storage_entry primary_entry,
    storage::storage_entry& tid,
    std::string* serialized = nullptr
);

/**
 * @brief Acquire a table-level DDL lock by looking up the table by name.
 * @param context request context
 * @param table_name name of the table to lock
 * @param [out] out storage entry of the table; valid only when true is returned
 * @return true if the lock is acquired; false with context error set if table is not found or lock is blocked
 */
bool acquire_table_lock(
    request_context& context,
    std::string_view table_name,
    storage::storage_entry& out
);

} // namespace jogasaki::executor::common
