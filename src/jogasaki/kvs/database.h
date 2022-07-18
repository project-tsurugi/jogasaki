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

#include <memory>
#include <glog/logging.h>

#include <takatori/util/fail.h>

#include <sharksfin/api.h>

#include "storage.h"
#include <jogasaki/common_types.h>
#include <jogasaki/logship/log_event_listener.h>

namespace jogasaki::kvs {

using takatori::util::fail;

using sharksfin::DatabaseHandle;
class transaction;


/**
 * @brief represent database in the transactional storage engine
 * @description This object is thread safe, except creating/deleting storages.
 * A database object created by open() call can be shared by multiple threads.
 * Storages (the table entries, not their content) are expected to be prepared sequentially before any concurrent access starts.
 */
class database {
public:
    /**
     * @brief create empty object
     */
    database() = default;

    /**
     * @brief create new object with existing db handle
     * @details the DatabaseHandle is simply borrowed by default, and no close/dispose calls will be made even if
     * this object is closed or destructed.
     */
    explicit database(DatabaseHandle handle) : handle_(handle) {}

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
    [[nodiscard]] static std::unique_ptr<database> open(std::map<std::string, std::string> const& options = {});

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
     * @param readonly whether the transaction is read-only
     * @param is_long whether the transaction is long batch
     * @param write_preserves write preserve storage names
     * @return transaction object
     */
    [[nodiscard]] std::unique_ptr<transaction> create_transaction(
        bool readonly = false,
        bool is_long = false,
        std::vector<std::string> const& write_preserve = {}
    );

    /**
     * @brief create new storage on the database
     * @param name name of the newly created storage
     * @return storage object for the newly created one
     * @return nullptr if the storage with given name already exists
     * @attention Concurrent operations for adding/removing storage entries are not strictly controlled for safety.
     * For the time being, storages are expected to be created sequentially before any transactions are started.
     */
    std::unique_ptr<storage> create_storage(std::string_view name);

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
     * @brief create new sequence
     * @returns the newly assigned sequence id
     */
    [[nodiscard]] sequence_id create_sequence() noexcept;

    /**
     * @brief update sequence value and version
     * @details request the transaction engine to make the sequence value for the specified version durable together
     * with the associated transaction.
     * @param id the sequence id whose value/version will be updated
     * @param version the version of the sequence value
     * @param value the new sequence value
     * @return true if successful, or false otherwise
     * @warning multiple put calls to a sequence with same version number cause undefined behavior.
     */
    [[nodiscard]] bool update_sequence(
        transaction& tx,
        sequence_id id,
        sequence_version version,
        sequence_value value) noexcept;

    /**
     * @brief get sequence value
     * @details retrieve sequence value of the "latest" version from the transaction engine.
     * @param id the sequence id whose value/version are to be retrieved
     * @returns versioned value that holds the sequence's latest version number and value
     */
    [[nodiscard]] sequence_versioned_value read_sequence(sequence_id id) noexcept;

    /**
     * @brief delete the sequence
     * @param handle the database handle where the sequence exists
     * @param id the sequence id that will be deleted
     * @return true if successful, or false otherwise
     */
    [[nodiscard]] bool delete_sequence(sequence_id id) noexcept;

    /**
     * @brief setter for log event listener
     * @param listener listener object to be called back when log event occurs
     */
    void log_event_listener(std::unique_ptr<logship::log_event_listener> listener);

private:
    sharksfin::DatabaseHandle handle_{};
    bool handle_borrowed_{true};
    std::unique_ptr<logship::log_event_listener> listener_{};
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

}

