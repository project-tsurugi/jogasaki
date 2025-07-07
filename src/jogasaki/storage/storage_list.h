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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <takatori/util/sequence_view.h>

namespace jogasaki::storage {

/**
 * @brief storage entry type
 * @details this type identifies a storage entry in the storage list.
 * Currently, only table (primary index) is stored in the list.
 * When we support other storage types, such as index and sequence, we need to pair
 * the id with the storage kind.
 */
using storage_entry = std::uint64_t;

class storage_list {
public:
    /**
     * @brief create empty object
     */
    storage_list() = default;

    /**
     * @brief destruct the object
     */
    ~storage_list() = default;

    storage_list(storage_list const& other) = default;
    storage_list& operator=(storage_list const& other) = default;
    storage_list(storage_list&& other) noexcept = default;
    storage_list& operator=(storage_list&& other) noexcept = default;

    /**
     * @brief create new object
     */
    storage_list(std::initializer_list<storage_entry> arg) noexcept :
        entries_(arg)
    {}

    /**
     * @brief create new object
     */
    explicit storage_list(std::vector<storage_entry> arg) noexcept :
        entries_(std::move(arg))
    {}

    void reserve(std::size_t sz) noexcept {
        entries_.reserve(sz);
    }

    void add(storage_entry entry) noexcept {
        entries_.emplace_back(entry);
    }

    [[nodiscard]] std::vector<storage_entry> const& entries() const noexcept {
        return entries_;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return entries_.size();
    }
private:
    std::vector<storage_entry> entries_{};
};

class storage_list_view {
public:
    /**
     * @brief create empty object
     */
    storage_list_view() = default;

    /**
     * @brief destruct the object
     */
    ~storage_list_view() = default;

    storage_list_view(storage_list_view const& other) = default;
    storage_list_view& operator=(storage_list_view const& other) = default;
    storage_list_view(storage_list_view&& other) noexcept = default;
    storage_list_view& operator=(storage_list_view&& other) noexcept = default;

    /**
     * @brief create new object
     */
    storage_list_view(storage_list const& list) noexcept :  //NOLINT(google-explicit-constructor,hicpp-explicit-conversions)
        entries_(list.entries())
    {}

    [[nodiscard]] takatori::util::sequence_view<storage_entry const> entity() const noexcept {
        return entries_;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return entries_.size();
    }

    [[nodiscard]] bool contains(storage_entry arg) const noexcept {
        return std::any_of(entries_.begin(), entries_.end(), [&](auto const& e) {
            return e == arg;
        });
    }

private:
    takatori::util::sequence_view<storage_entry const> entries_{};
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(storage_list_view const& a, storage_list_view const& b) noexcept {
    if (a.size() != b.size()) {
        return false;
    }
    for(std::size_t i=0, n=a.size(); i<n; ++i) {
        if (a.entity()[i] != b.entity()[i]) {
            return false;
        }
    }
    return true;
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(storage_list_view const& a, storage_list_view const& b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @tparam T the element type
 * @param out the output stream
 * @param value the target value
 * @return the output stream
 */
inline std::ostream& operator<<(std::ostream& out, storage_list_view const& value) {
    return out << value.entity();
}

} // namespace jogasaki::storage

