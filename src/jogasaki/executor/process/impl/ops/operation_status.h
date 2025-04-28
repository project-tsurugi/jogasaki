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

#include <jogasaki/request_context.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include "operator_kind.h"

namespace jogasaki::executor::process::impl {
class variable_table;
}

namespace jogasaki::executor::process::impl::ops {

enum class operation_status_kind {
    /**
     * @brief the operation completed normally
     */
    ok,

    /**
     * @brief the operation aborted
     */
    aborted,
    /**
     * @brief the operation yield
     */
    yield,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
constexpr inline std::string_view to_string_view(operation_status_kind value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case operation_status_kind::ok: return "ok"sv;
        case operation_status_kind::aborted: return "aborted"sv;
        case operation_status_kind::yield: return "yield"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, operation_status_kind value) {
    return out << to_string_view(value);
}

/**
 * @brief relational operator execution status
 */
class operation_status {
public:
    /**
     * @brief create empty object
     */
    constexpr operation_status() = default;

    /**
     * @brief create new object
     */
    constexpr operation_status(  //NOLINT
        operation_status_kind kind
    ) :
        kind_(kind)
    {}

    operation_status(operation_status const& other) = default;
    operation_status& operator=(operation_status const& other) = default;
    operation_status(operation_status&& other) noexcept = default;
    operation_status& operator=(operation_status&& other) noexcept = default;

    /**
     * @brief destory the object
     */
    ~operation_status() = default;

    /**
     * @brief accessor for operator kind
     */
    [[nodiscard]] constexpr operation_status_kind kind() const noexcept {
        return kind_;
    }

    /**
     * @brief accessor to request context
     * @return the request context
     */
    [[nodiscard]] constexpr operator bool() noexcept {  //NOLINT
        return kind_ == operation_status_kind::ok;
    }

private:
    operation_status_kind kind_{operation_status_kind::ok};
};

/**
 * @brief equality comparison operator
 */
constexpr inline bool operator==(operation_status const& a, operation_status const& b) noexcept {
    return a.kind() == b.kind();
}

/**
 * @brief inequality comparison operator
 */
constexpr inline bool operator!=(operation_status const& a, operation_status const& b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, operation_status value) {
    return out << value.kind();
}

static_assert(std::is_trivially_destructible_v<operation_status>);
static_assert(std::is_trivially_copyable_v<operation_status>);

}


