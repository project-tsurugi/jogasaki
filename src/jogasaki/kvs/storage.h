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
#include <memory>
#include <ostream>
#include <string_view>
#include <glog/logging.h>

#include <sharksfin/StorageOptions.h>
#include <sharksfin/api.h>

#include <jogasaki/lob/lob_id.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/fail.h>

#include "transaction.h"

namespace jogasaki::kvs {

class database;
class iterator;

enum class end_point_kind : std::uint32_t {
    unbound = 0U,
    inclusive,
    exclusive,
    prefixed_inclusive,
    prefixed_exclusive,
};

enum class put_option : std::uint32_t {
    /**
     * @brief to update the existing entry, or create new one if the entry doesn't exist.
     */
    create_or_update = 0U,

    /**
     * @brief to create new entry. status::already_exists is returned from put operation if the entry already exist.
     */
    create,

    /**
     * @brief to update existing entry. status::not_found is returned from put operation if the entry doesn't exist.
     */
    update,
};

/**
 * @brief storage object in the database
 * @details storage typically represents a table on the database in the transaction engine layer.
 * This object is simply a wrapper for sharksfin::StorageHandle and assumed to be usable concurrently from multiple threads
 * as long as the involved transactions are different. Exceptions are object creation/destruction/set_options that are
 * expected to be one time operation in object life-time.
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
    explicit storage(sharksfin::StorageHandle handle) noexcept;

    /**
     * @brief destruct object
     */
    ~storage() noexcept;

    storage(storage const& other) = delete;
    storage& operator=(storage const& other) = delete;
    storage(storage&& other) noexcept = delete;
    storage& operator=(storage&& other) noexcept = delete;

    /**
     * @brief return the native handle in the transaction layer
     * @note this is expected to be package private (i.e. callable from code in kvs namespace)
     * @return the handle held by this object
     */
    [[nodiscard]] sharksfin::StorageHandle handle() const noexcept;

    /**
     * @brief delete the storage
     * @return status::ok if the storage is successfully deleted
     * @return otherwise, other status code
     * @attention Concurrent operations for adding/removing storage entries are not strictly controlled for safety.
     * For the time being, storages are expected to be created sequentially before any transactions are started.
     * Accessing the storage object which is deleted by this call causes undefined behavior.
     */
    [[nodiscard]] status delete_storage();

    /**
     * @brief scan the storage with given key conditions
     * @param tx transaction used for the scan
     * @param begin_key the begin key
     * @param begin_kind endpoint of the begin key
     * @param end_key the end key
     * @param end_kind endpoint of the end key
     * @param it[out] iterator for the scan result
     * @param limit the maximum number of entries to scan
     * @param reverse if true, the scan is done in reverse order (from end to begin)
     * @return status::ok if the operation is successful
     * @return status::err_invalid_key_length if the given key has invalid length to be handled by kvs
     * @return otherwise, other status code
     * @note this function just prepares iterator without starting scan, so status::not_found is not returned.
     */
    [[nodiscard]] status content_scan(
        transaction& tx,
        std::string_view begin_key, end_point_kind begin_kind,
        std::string_view end_key, end_point_kind end_kind,
        std::unique_ptr<iterator>& it,
        std::size_t limit = 0,
        bool reverse = false
    );
    /**
     * @brief get the value for the given key
     * @param tx transaction used for the point query
     * @param key key for searching
     * @param value[out] the value of the entry matching the key
     * The data pointed by the returned value gets invalidated after any other api call.
     * @return status::ok if the operation is successful
     * @return status::not_found if the entry for the key is not found
     * @return status::concurrent_operation if other concurrent operation is observed and the request is rejected.
     * The transaction is still active (i.e. not aborted). Retrying the request might be successful if the concurrent
     * operation complete, or doesn't exist any more.
     * @return status::err_serialization_failure on early abort
     * @return status::err_invalid_key_length if the given key has invalid length to be handled by kvs
     * @return otherwise, other status code
     */
    [[nodiscard]] status content_get(
        transaction& tx,
        std::string_view key,
        std::string_view& value
    );

    /**
     * @brief put the value for the given key
     * @param tx transaction used
     * @param key the key for the entry
     * @param value the value for the entry
     * @param option option to set put mode
     * @return status::ok if the operation is successful
     * @return status::already_exists if the option is `create` and record already exists for the key
     * @return status::not_found if the option is `update` and the record doesn't exist for the key
     * @return status::concurrent_operation if `option` is `create` and other concurrent operation is observed.
     * The transaction is still active (i.e. not aborted). Retrying the request might be successful if the concurrent
     * operation complete, or doesn't exist any more.
     * @return status::err_serialization_failure on early abort
     * @return status::err_write_operation_by_rtx if transaction is read-only
     * @return status::err_invalid_key_length if the given key has invalid length to be handled by kvs
     * @return otherwise, other status code
     */
    [[nodiscard]] status content_put(
        transaction& tx,
        std::string_view key,
        std::string_view value,
        put_option option = put_option::create_or_update,
        std::vector<lob::lob_id_type> const& lobs = {}
    );

    /**
     * @brief remove the entry for the given key
     * @param tx transaction used for the delete operation
     * @param key the key for searching
     * @return status::ok if the operation is successful
     * @return status::not_found if the entry for the key is not found
     * @return status::err_serialization_failure on early abort
     * @return status::err_write_operation_by_rtx if transaction is read-only
     * @return status::err_invalid_key_length if the given key has invalid length to be handled by kvs
     * @return otherwise, other status code
     */
    [[nodiscard]] status content_delete(
        transaction& tx,
        std::string_view key
    );

    /**
     * @brief set the storage options
     * @param options storage options to set
     * @param tx transaction used
     * @note this method is thread unsafe and should not be called simultaneously from multiple threads.
     * This is expected to be called only once during the object initialization. Do not race with `get_options`.
     * @return status::ok if the operation is successful
     * @return otherwise, other status code
     */
    [[nodiscard]] status set_options(
        sharksfin::StorageOptions const& options
    );

    /**
     * @brief get the storage options
     * @return status::ok if the operation is successful
     * @return otherwise, other status code
     */
    [[nodiscard]] status get_options(
        sharksfin::StorageOptions& options
    );

private:
    sharksfin::StorageHandle handle_{};

    sharksfin::EndPointKind kind(end_point_kind k);
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
/**
 * @brief Adjusts the endpoint kind based on whether a secondary index is used.
 *
 * @details This function modifies the given endpoint kind depending on whether a secondary index
 * is being used. If `use_secondary` is true and the endpoint is either inclusive or exclusive,
 * it adjusts the endpoint kind to a prefixed version (i.e., `prefixed_inclusive` or `prefixed_exclusive`).
 * Otherwise, the original endpoint kind is returned.
 *
 * @param use_secondary Indicates if a secondary index is being used.
 * @param endpoint The original endpoint kind to be adjusted.
 * @return The adjusted endpoint kind based on the `use_secondary` flag.
 */
end_point_kind adjust_endpoint_kind(bool use_secondary, kvs::end_point_kind endpoint);
/**
 * @brief Outputs a string representation of the `end_point_kind` to the provided output stream.
 *
 * @details This overloaded `operator<<` allows for `end_point_kind` values to be output to standard streams
 * (such as `std::cout`) in a human-readable form. It handles each possible enum value and outputs
 * the corresponding string (e.g., "inclusive", "exclusive", etc.). If an unknown value is passed,
 * "unknown" is output.
 *
 * @param os The output stream where the string representation will be written.
 * @param kind The `end_point_kind` value to be converted to a string.
 * @return The output stream with the string representation appended.
 */
std::ostream& operator<<(std::ostream& os, end_point_kind kind);

} // namespace jogasaki::kvs
