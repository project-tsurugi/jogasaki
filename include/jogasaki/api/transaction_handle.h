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
 * @brief transaction handle
 * @details the handle is the trivially copyable object that references transaction object stored in the database.
 * Using the handle, caller can create, execute, and destroy the transaction keeping the ownership managed
 * by the database. This makes api more flexible compared to handling the unique_ptr to prepared statement.
 */
class transaction_handle {
public:

    /**
     * @brief create empty handle - null reference
     */
    transaction_handle() = default;

    /**
     * @brief destruct the object
     */
    ~transaction_handle() = default;


    transaction_handle(transaction_handle const& other) = default;
    transaction_handle& operator=(transaction_handle const& other) = default;
    transaction_handle(transaction_handle&& other) noexcept = default;
    transaction_handle& operator=(transaction_handle&& other) noexcept = default;

    /**
     * @brief create new object from pointer
     * @param ptr the target prepared statement
     */
    explicit transaction_handle(transaction* ptr) noexcept :
        ptr_(ptr)
    {}

    /**
     * @brief accessor to the referenced prepared statement
     * @return the target prepared statement pointer
     */
    [[nodiscard]] api::transaction* get() const noexcept {
        return ptr_;
    }

    /**
     * @brief accessor to the referenced prepared statement
     * @return the target prepared statement pointer
     */
    [[nodiscard]] api::transaction* operator->() const noexcept {
        return ptr_;
    }

    /**
     * @brief conversion operator to std::size_t
     * @return the hash value that can be used for equality comparison
     */
    explicit operator std::size_t() const noexcept {
        return reinterpret_cast<std::size_t>(ptr_);  //NOLINT
    }

    /**
     * @brief conversion operator to bool
     * @return whether the handle has body (i.e. referencing valid transaction) or not
     */
    explicit operator bool() const noexcept {
        return ptr_ != nullptr;
    }
private:
    api::transaction* ptr_{};

};

static_assert(std::is_trivially_copyable_v<transaction_handle>);

/**
 * @brief equality comparison operator
 */
inline bool operator==(transaction_handle const& a, transaction_handle const& b) noexcept {
    return a.get() == b.get();
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(transaction_handle const& a, transaction_handle const& b) noexcept {
    return !(a == b);
}

}
/**
 * @brief std::hash specialization
 */
template<>
struct std::hash<jogasaki::api::transaction_handle> {
    /**
     * @brief compute hash of the given object.
     * @param value the target object
     * @return computed hash code
     */
    std::size_t operator()(jogasaki::api::transaction_handle const& value) const noexcept {
        return static_cast<std::size_t>(value);
    }
};

