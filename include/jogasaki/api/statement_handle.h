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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <ostream>
#include <string_view>
#include <type_traits>

#include <jogasaki/api/record_meta.h>
#include <jogasaki/utils/hash_combine.h>
#include <jogasaki/utils/split_mix64.h>

namespace jogasaki::api {

/**
 * @brief prepared statement handle
 * @details the handle is the trivially copyable object that references prepared statement stored in the database.
 * Using the handle, caller can create, execute, and destroy the prepared statement keeping the ownership managed
 * by the database. This makes api more flexible compared to handling the unique_ptr to prepared statement.
 */
class statement_handle {
public:
    /**
     * @brief create empty handle - null reference
     */
    statement_handle() = default;

    /**
     * @brief destruct the object
     */
    ~statement_handle() = default;

    statement_handle(statement_handle const& other) = default;
    statement_handle& operator=(statement_handle const& other) = default;
    statement_handle(statement_handle&& other) noexcept = default;
    statement_handle& operator=(statement_handle&& other) noexcept = default;

    /**
     * @brief create new object from pointer
     * @param arg pointer to the target prepared statement
     * @param session_id the session id of the prepared statement
     */
    statement_handle(
        void* arg,
        std::optional<std::size_t> session_id
    ) noexcept;

    /**
     * @brief accessor to the referenced prepared statement
     * @return the target prepared statement pointer
     */
    [[nodiscard]] std::uintptr_t get() const noexcept;

    /**
     * @brief return the session id of the transaction
     * @return session id
     */
    [[nodiscard]] std::optional<std::size_t> session_id() const noexcept;

    /**
     * @brief conversion operator to std::size_t
     * @return the hash value that can be used for equality comparison
     */
    explicit operator std::size_t() const noexcept;

    /**
     * @brief conversion operator to bool
     * @return whether the handle has body (i.e. referencing valid statement) or not
     */
    explicit operator bool() const noexcept;

    /**
     * @brief accessor to output meta data
     * @return the record meta data if the statement has output data
     * @return nullptr otherwise
     */
    [[nodiscard]] api::record_meta const* meta() const noexcept;

    /**
     * @brief accessor to has-result-records flag
     * @return whether the prepared statement possibly has result records (e.g. query)
     */
    [[nodiscard]] bool has_result_records() const noexcept;

private:
    std::uintptr_t body_{};
    std::optional<std::size_t> session_id_{};

};

static_assert(std::is_trivially_copyable_v<statement_handle>);

/**
 * @brief equality comparison operator
 */
inline bool operator==(statement_handle const& a, statement_handle const& b) noexcept {
    if (a.get() != b.get()) {
        return false;
    }
    if (a.session_id().has_value() != b.session_id().has_value()) {
        return false;
    }
    if (! a.session_id().has_value()) {
        return true;
    }
    return *a.session_id() == *b.session_id();
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(statement_handle const& a, statement_handle const& b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, statement_handle value) {
    // body is printed in decimal to avoid using utils::hex in public header
    out << "statement_handle[body:" << value.get();
    if (value.session_id().has_value()) {
        // print session_id in decimal to conform to tateyama log message
        out << ",session_id:" << value.session_id().value();
    }
    out << "]";
    return out;
}

}  // namespace jogasaki::api

/**
 * @brief std::hash specialization
 */
template<>
struct std::hash<jogasaki::api::statement_handle> {
    /**
     * @brief compute hash of the given object.
     * @param value the target object
     * @return computed hash code
     */
    std::size_t operator()(jogasaki::api::statement_handle const& value) const noexcept {
        // mixing pointer value to avoid collisions
        auto h = jogasaki::utils::split_mix64(value.get());
        if (! value.session_id().has_value()) {
            // normally we don't mix entries with/without session_id, so this doesn't make many collisions
            return h;
        }
        return jogasaki::utils::hash_combine(h, *value.session_id());
    }
};

