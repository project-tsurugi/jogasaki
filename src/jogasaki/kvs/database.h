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
     * @brief create new object
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
     * @brief create new object
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
     * @return transaction object
     */
    [[nodiscard]] std::unique_ptr<transaction> create_transaction();

    /**
     * @brief create new storage on the database
     * @param name name of the newly created storage
     * @return storage object for the newly created one
     * @return nullptr if the storage with given name already exists
     * @attention Concurrent operations for adding/removing storage entries are not strictly controlled for safety.
     * For the time being, storages are expected to be created sequentially before any transactions are started.
     */
    std::unique_ptr<storage> create_storage(std::string_view name) {
        sharksfin::StorageHandle stg{};
        if (auto res = sharksfin::storage_create(handle_, sharksfin::Slice(name), &stg); res == sharksfin::StatusCode::ALREADY_EXISTS) {
            return {};
        } else if (res != sharksfin::StatusCode::OK) { //NOLINT
            fail();
        }
        return std::make_unique<storage>(stg);
    }

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
    std::unique_ptr<storage> get_storage(std::string_view name) {
        sharksfin::StorageHandle stg{};
        if (auto res = sharksfin::storage_get(handle_, sharksfin::Slice(name), &stg); res == sharksfin::StatusCode::NOT_FOUND) {
            return {};
        } else if (res != sharksfin::StatusCode::OK) { //NOLINT
            fail();
        }
        return std::make_unique<storage>(stg);
    }

private:
    sharksfin::DatabaseHandle handle_{};
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

