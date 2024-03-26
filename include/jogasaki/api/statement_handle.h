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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <ostream>
#include <string_view>
#include <type_traits>

#include <jogasaki/api/record_meta.h>

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
     */
    explicit statement_handle(void* arg) noexcept;

    /**
     * @brief create new object from integer
     * @param arg integer representing target pointer
     */
    explicit statement_handle(std::uintptr_t arg) noexcept;

    /**
     * @brief accessor to the referenced prepared statement
     * @return the target prepared statement pointer
     */
    [[nodiscard]] std::uintptr_t get() const noexcept;

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

};

static_assert(std::is_trivially_copyable_v<statement_handle>);

/**
 * @brief equality comparison operator
 */
inline bool operator==(statement_handle const& a, statement_handle const& b) noexcept {
    return a.get() == b.get();
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
    return out << "statement_handle[" << value.get() << "]";
}

}
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
        return static_cast<std::size_t>(value);
    }
};

