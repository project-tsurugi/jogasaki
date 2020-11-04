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
#include "transaction.h"

namespace jogasaki::kvs {
using takatori::util::fail;

class database;
class iterator;

enum class end_point_kind : std::uint32_t {
    unbound = 0U,
    inclusive,
    exclusive,
    prefixed_inclusive,
    prefixed_exclusive,
};
/**
 * @brief storage object in the database
 * @details storage typically represents a table on the database in the transaction engine layer.
 * The object is thread unsafe, and should not be called from different threads simultaneously.
 */
class storage {
public:
    /**
     * @brief create empty object
     */
    storage() = default;

    /**
     * @brief create new object
     */
    explicit storage(sharksfin::StorageHandle handle) noexcept : handle_(handle) {}

    /**
     * @brief destruct object
     */
    ~storage() noexcept {
        sharksfin::storage_dispose(handle_);
    }

    storage(storage const& other) = delete;
    storage& operator=(storage const& other) = delete;
    storage(storage&& other) noexcept = delete;
    storage& operator=(storage&& other) noexcept = delete;

    /**
     * @brief return the native handle in the transaction layer
     * @note this is expected to be package private (i.e. callable from code in kvs namespace)
     * @return the handle held by this object
     */
    [[nodiscard]] sharksfin::StorageHandle handle() const noexcept {
        return handle_;
    }

    /**
     * @brief delete the storage
     * @return true if the storage is successfully deleted
     * @return false any other error happens
     * @attention Concurrent operations for adding/removing storage entries are not strictly controlled for safety.
     * For the time being, storages are expected to be created sequentially before any transactions are started.
     * Accessing the storage object which is deleted by this call causes undefined behavior.
     */
    [[nodiscard]] bool delete_storage();

    /**
     * @brief scan the storage with given key conditions
     * @param tx transaction used for the scan
     * @param begin_key the begin key
     * @param begin_kind endpoint of the begin key
     * @param end_key the end key
     * @param end_kind endpoint of the end key
     * @param it[out] iterator for the scan result
     * @return true if the operation is successful
     * @return false otherwise
     */
    [[nodiscard]] bool scan(
        transaction& tx,
        std::string_view begin_key, end_point_kind begin_kind,
        std::string_view end_key, end_point_kind end_kind,
        std::unique_ptr<iterator>& it
    );

    /**
     * @brief get the value for the given key
     * @param tx transaction used for the point query
     * @param key key for searching
     * @param value[out] the value of the entry matching the key
     * The data pointed by the returned value gets invalidated after any other api call.
     * @return true if the operation is successful
     * @return false otherwise (e.g. the entry for the key is not found)
     */
    [[nodiscard]] bool get(
        transaction& tx,
        std::string_view key,
        std::string_view& value
    );

    /**
     * @brief put the value for the given key (create new entry if it does not exist, otherwise update)
     * @param tx transaction used
     * @param key the key for the entry
     * @param value the value for the entry
     * @return true if the operation is successful
     * @return false otherwise
     */
    [[nodiscard]] bool put(
        transaction& tx,
        std::string_view key,
        std::string_view value
    );

    /**
     * @brief remove the entry for the given key
     * @param tx transaction used for the delete operation
     * @param key the key for searching
     * @return true if the operation is successful
     * @return false otherwise
     */
    [[nodiscard]] bool remove(
        transaction& tx,
        std::string_view key
    );

private:
    sharksfin::StorageHandle handle_{};

    sharksfin::EndPointKind kind(end_point_kind k) {
        return sharksfin::EndPointKind(static_cast<std::uint32_t>(k));
    }
};

/**
 * @brief compare contents of two objects
 * @param a first arg to compare
 * @param b second arg to compare
 * @return true if a == b
 * @return false otherwise
 */
inline bool operator==(storage const& a, storage const& b) noexcept {
    return a.handle() == b.handle();
}
inline bool operator!=(storage const& a, storage const& b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, storage const& value) {
    out << "storage(handle:" << std::hex << value.handle() << ")";
    return out;
}

}

