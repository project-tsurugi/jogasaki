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

#include <memory>

#include <limestone/api/datastore.h>

namespace jogasaki::datastore {

/**
 * @brief datastore kind
 */
enum class datastore_kind : std::int32_t {
    undefined = 0,
    production,
    mock,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(datastore_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = datastore_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::production: return "production"sv;
        case kind::mock: return "mock"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, datastore_kind value) {
    return out << to_string_view(value);
}

class datastore {
public:
    /**
     * @brief create empty object
     */
    datastore() noexcept = default;

    /**
     * @brief destruct the object
     */
    virtual ~datastore() noexcept = default;

    datastore(datastore const& other) = default;
    datastore& operator=(datastore const& other) = default;
    datastore(datastore&& other) noexcept = default;
    datastore& operator=(datastore&& other) noexcept = default;

    /**
     * @brief return the kind of this object
     */
    virtual datastore_kind kind() noexcept = 0;

    /**
     * @brief acquires a new empty BLOB pool.
     * @details This pool is used for temporary registration of BLOBs,
     *      and all BLOBs that are not fully registered will become unavailable when the pool is destroyed.
     * @return the created BLOB pool
     * @see blob_pool::release()
     * @attention the returned BLOB pool must be released by the blob_pool::release() after the usage, or it may cause leaks of BLOB data.
     * @attention Undefined behavior if using pool after destroying this datastore.
     */
    [[nodiscard]] virtual std::unique_ptr<limestone::api::blob_pool> acquire_blob_pool() = 0;

    /**
     * @brief returns BLOB file for the BLOB reference.
     * @param reference the target BLOB reference
     * @return the corresponding BLOB file
     * @return unavailable BLOB file if there is no BLOB file for the reference,
     *   that is, the BLOB file has not been registered or has already been removed.
     * @attention the returned BLOB file is only effective
     *    during the transaction that has provided the corresponding BLOB reference.
     */
    [[nodiscard]] virtual limestone::api::blob_file get_blob_file(limestone::api::blob_id_type reference) = 0;
};

}  // namespace jogasaki::datastore
