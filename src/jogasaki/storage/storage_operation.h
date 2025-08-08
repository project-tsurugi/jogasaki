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

#include <vector>
#include <utility>
#include <cstddef>
#include <iterator>
#include <jogasaki/storage/storage_list.h>
#include <jogasaki/auth/action_set.h>

namespace jogasaki::storage {

/**
 * @brief Represents a set of storage operations, pairing storage ids with their action sets.
 */
class storage_operation {
public:
    using entry_type = std::pair<storage_entry, auth::action_set&>;

    /**
     * @brief Iterator for storage_operation
     */
    class iterator {
    public:
        using difference_type = std::ptrdiff_t;
        using value_type = std::pair<storage_entry, auth::action_set>;
        using pointer = std::pair<storage_entry, auth::action_set&>*;
        using reference = std::pair<storage_entry, auth::action_set&>;
        using iterator_category = std::forward_iterator_tag;

        iterator(
            decltype(std::declval<storage_list>().begin()) entry_it,
            decltype(std::declval<std::vector<auth::action_set>>().begin()) action_it
        ) :
            entry_it_(entry_it),
            action_it_(action_it)
        {}

        reference operator*() const {
            return {*entry_it_, *action_it_};
        }

        iterator& operator++() {
            ++entry_it_;
            ++action_it_;
            return *this;
        }

        iterator operator++(int) {
            iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        bool operator==(iterator const& other) const {
            return entry_it_ == other.entry_it_ && action_it_ == other.action_it_;
        }

        bool operator!=(iterator const& other) const {
            return !(*this == other);
        }

    private:
        decltype(std::declval<storage_list>().begin()) entry_it_;
        decltype(std::declval<std::vector<auth::action_set>>().begin()) action_it_;
    };

    /**
     * @brief Const iterator for storage_operation
     */
    class const_iterator {
    public:
        using difference_type = std::ptrdiff_t;
        using value_type = std::pair<storage_entry, const auth::action_set>;
        using pointer = std::pair<storage_entry, const auth::action_set&>*;
        using reference = std::pair<storage_entry, const auth::action_set&>;
        using iterator_category = std::forward_iterator_tag;

        const_iterator(
            decltype(std::declval<storage_list>().cbegin()) entry_it,
            decltype(std::declval<std::vector<auth::action_set>>().cbegin()) action_it
        ) :
            entry_it_(entry_it),
            action_it_(action_it)
        {}

        reference operator*() const {
            return {*entry_it_, *action_it_};
        }

        const_iterator& operator++() {
            ++entry_it_;
            ++action_it_;
            return *this;
        }

        const_iterator operator++(int) {
            const_iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        bool operator==(const_iterator const& other) const {
            return entry_it_ == other.entry_it_ && action_it_ == other.action_it_;
        }

        bool operator!=(const_iterator const& other) const {
            return !(*this == other);
        }

    private:
        decltype(std::declval<storage_list>().cbegin()) entry_it_;
        decltype(std::declval<std::vector<auth::action_set>>().cbegin()) action_it_;
    };

    storage_operation() = default;

    storage_operation(storage_list list, std::vector<auth::action_set> actions)
        : storages_(std::move(list)), actions_(std::move(actions))
    {}

    [[nodiscard]] storage_list_view storage() const noexcept {
        return storage_list_view(storages_);
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return storages_.size();
    }

    [[nodiscard]] iterator begin() noexcept {
        return iterator(storages_.begin(), actions_.begin());
    }

    [[nodiscard]] iterator end() noexcept {
        return iterator(storages_.end(), actions_.end());
    }

    [[nodiscard]] const_iterator begin() const noexcept {
        return const_iterator(storages_.cbegin(), actions_.cbegin());
    }

    [[nodiscard]] const_iterator end() const noexcept {
        return const_iterator(storages_.cend(), actions_.cend());
    }

    [[nodiscard]] const_iterator cbegin() const noexcept {
        return begin();
    }

    [[nodiscard]] const_iterator cend() const noexcept {
        return end();
    }

private:
    storage_list storages_{};
    std::vector<auth::action_set> actions_{};
};

} // namespace jogasaki::storage
