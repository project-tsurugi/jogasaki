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

namespace jogasaki::api {

/**
 * @brief prepared statement handle
 */
class statement_handle {
public:
    statement_handle() = default;
    ~statement_handle() = default;
    statement_handle(statement_handle const& other) = default;
    statement_handle& operator=(statement_handle const& other) = default;
    statement_handle(statement_handle&& other) noexcept = default;
    statement_handle& operator=(statement_handle&& other) noexcept = default;

    explicit statement_handle(prepared_statement* ptr) noexcept :
        ptr_(ptr)
    {}

    [[nodiscard]] prepared_statement* get() const noexcept {
        return ptr_;
    }

    explicit operator std::size_t() const noexcept {
        return reinterpret_cast<std::size_t>(ptr_);
    }

private:
    prepared_statement* ptr_{};

};

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

