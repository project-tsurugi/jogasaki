/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <cstdint>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>
#include <glog/logging.h>

#include <sharksfin/StorageOptions.h>
#include <sharksfin/api.h>

#include <jogasaki/common_types.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/transaction_option.h>
#include <jogasaki/lob/lob_id.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/fail.h>

#include "storage.h"

namespace jogasaki::kvs {

using sharksfin::DatabaseHandle;
class transaction;

// ensure jogasaki blob id compatible with sharksfin
static_assert(std::is_same_v<lob::lob_id_type, ::sharksfin::blob_id_type>);

/**
 * @brief represent database in the transactional storage engine
 * @description This object is thread safe, except creating/deleting storages.
 * A database object created by open() call can be shared by multiple threads.
 * Storages (the table entries, not their content) are expected to be prepared sequentially before any concurrent access starts.
 */
class database {
public:
    static constexpr std::uint64_t undefined_storage_id = static_cast<std::uint64_t>(-1);

    /**
     * @brief create empty object
     */
    database();

    /**
     * @brief create new object with existing db handle
     * @details the DatabaseHandle is simply borrowed by default, and no close/dispose calls will be made even if
     * this object is closed or destructed.
     */
    explicit database(DatabaseHandle handle);

    /**
     * @brief destruct the object
     */
    ~database() noexcept;

    database(database const& other) = delete;
    database& operator=(database const& other) = delete;
    database(database&& other) noexcept = delete;
    database& operator=(database&& other) noexcept = delete;

    /**
     * @brief create new object with new kvs instance
     * @details contrary to constructor `database(DatabaseHandle handle)`, opened kvs db instance will be owned by the
     * returned object, and close/dispose calls to DatabaseHandle will be made when the returned object is closed or
     * destructed.
     */
    [[nodiscard]] static std::shared_ptr<database> open(std::map<std::string, std::string> const& options = {});

    /**
     * @brief close the database
     * @details stop using and close the database. Further access to the database object after this call causes undefined behavior.
     * @return true if the operation is successful
     * @return false otherwise
     * @attention the concurrent access for this call are not strictly controlled.
     * It's expected to be called from a single thread after all database activities are finished.
     */
    [[nodiscard]] bool close();

    /**
     * @brief return the native handle in the transaction layer
     * @note this is expected to be package private (i.e. callable from code in kvs namespace)
     * @return the handle held by this object
     */
    [[nodiscard]] sharksfin::DatabaseHandle handle() const noexcept;

    /**
     * @brief create and start new transaction
     * @param options transaction options
     * @return transaction object
     * @deprecated use `transaction::create_transaction(kvs::db&, std::unique_ptr<transaction>&, transaction_option const&)`.
     */
    [[nodiscard]] std::unique_ptr<transaction> create_transaction(
        transaction_option const& options = {}
    );

    /**
     * @brief create new storage on the database
     * @param name name of the newly created storage
     * @param options the options for the newly created storage
     * @return storage object for the newly created one
     * @return nullptr if the storage with given name already exists
     * @attention Concurrent operations for adding/removing storage entries are not strictly controlled for safety.
     * For the time being, storages are expected to be created sequentially before any transactions are started.
     */
    std::unique_ptr<storage> create_storage(std::string_view name, sharksfin::StorageOptions const& options = {});

    /**
     * @brief retrieve the storage on the database
     * @param name name of the storage
     * @return storage object for the given name
     * @return nullptr if the storage with given name does not exist
     * @attention Multiple threads can call this function simultaneously to get the storages and each thread can use one
     * retrieved to update storage content. That can be done concurrently.
     * But concurrent operations for adding/removing storage entries are not strictly controlled for safety.
     * For the time being, storages are expected to be created sequentially before any transactions are started.
     * Accessing the storage object which is deleted by storage::delete_storage() causes undefined behavior.
     */
    std::unique_ptr<storage> get_storage(std::string_view name);

    /**
     * @brief retrieve the storage on the database or create if not found
     * @param name name of the storage
     * @return storage object for the given name
     * @return nullptr if the any error occurs
     * @attention Multiple threads can call this function simultaneously to get the storages and each thread can use one
     * retrieved to update storage content. That can be done concurrently.
     * But concurrent operations for adding/removing storage entries are not strictly controlled for safety.
     * For the time being, storages are expected to be created sequentially before any transactions are started.
     * Accessing the storage object which is deleted by storage::delete_storage() causes undefined behavior.
     */
    std::unique_ptr<storage> get_or_create_storage(std::string_view name);

    /**
     * @brief list storages defined for the database
     */
    [[nodiscard]] status list_storages(std::vector<std::string>& out) const noexcept;

    /**
     * @brief create new sequence
     * @param out the newly assigned sequence id
     * @return status::ok if successful
     * @return any other error
     */
    [[nodiscard]] status create_sequence(sequence_id& out);

    /**
     * @brief update sequence value and version
     * @details request the transaction engine to make the sequence value for the specified version durable together
     * with the associated transaction.
     * @param id the sequence id whose value/version will be updated
     * @param version the version of the sequence value
     * @param value the new sequence value
     * @return status::ok if successful
     * @return status::err_not_found if sequence is not found. Passed transaction is aborted.
     * @return any other error
     * @warning multiple put calls to a sequence with same version number cause undefined behavior.
     */
    [[nodiscard]] status update_sequence(
        transaction& tx,
        sequence_id id,
        sequence_version version,
        sequence_value value) noexcept;

    /**
     * @brief get sequence value
     * @details retrieve sequence value of the "latest" version from the transaction engine.
     * @param id the sequence id whose value/version are to be retrieved
     * @param out versioned value that holds the sequence's latest version number and value
     * @return status::ok if successful
     * @return status::err_not_found if sequence is not found
     * @return any other error
     */
    [[nodiscard]] status read_sequence(sequence_id id, sequence_versioned_value& out);

    /**
     * @brief delete the sequence
     * @param handle the database handle where the sequence exists
     * @param id the sequence id that will be deleted
     * @return status::ok if successful
     * @return status::err_not_found if sequence is not found
     * @return any other error
     */
    [[nodiscard]] status delete_sequence(sequence_id id);

    /**
     * @brief register durability callback
     * @param the callback to be invoked when durability event (i.e. updates on durability marker) occurrs
     * @return status::ok if successful
     * @return any other error
     */
    status register_durability_callback(::sharksfin::durability_callback_type cb);

    /**
     * @brief get the datastore (limestone) object
     * @param out [out] the output parameter filled with the datastore object pointer
     * @return status::ok if successful
     * @return any other error
     */
    status get_datastore(std::any& out);

private:
    sharksfin::DatabaseHandle handle_{};
    bool handle_borrowed_{true};
};

/**
 * @brief compare contents of two objects
 * @param a first arg to compare
 * @param b second arg to compare
 * @return true if a == b
 * @return false otherwise
 */
inline bool operator==(database const& a, database const& b) noexcept {
    return a.handle() == b.handle();
}
inline bool operator!=(database const& a, database const& b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, database const& value) {
    out << "database(handle:" << std::hex << value.handle() << ")";
    return out;
}

}  // namespace jogasaki::kvs
