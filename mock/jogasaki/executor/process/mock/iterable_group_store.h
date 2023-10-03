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

#include <takatori/util/sequence_view.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/mock/group_reader.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/utils/copy_field_data.h>

namespace jogasaki::executor::process::mock {

using kind = meta::field_type_kind;

class cache_align iterable_group_store {
public:
    using group_type = basic_group_entry;
    using key_type = group_type::key_type;
    using values_type = group_type::value_groups;

    iterable_group_store() = default;

    iterable_group_store(
        key_type key,
        values_type values
    ) :
        key_(key),
        values_(std::move(values))
    {}

    void release() {
        // NOOP
    }

    ~iterable_group_store() = default;
    iterable_group_store(iterable_group_store const& other) = default;
    iterable_group_store& operator=(iterable_group_store const& other) = default;
    iterable_group_store(iterable_group_store&& other) noexcept = default;
    iterable_group_store& operator=(iterable_group_store&& other) noexcept = default;

    /**
     * @brief iterator for the stored records
     */
    class iterator {
    public:

        /// @brief iterator category
        using iterator_category = std::input_iterator_tag;

        /// @brief type of value
        using value_type = decltype(std::declval<iterable_group_store::group_type::value_type>().ref());

        /// @brief type of difference
        using difference_type = std::ptrdiff_t;

        /// @brief type of pointer
        using pointer = value_type*;

        /// @brief type of reference
        using reference = value_type&;

        /**
         * @brief construct new iterator
         * @param container the target record store that the constructed object iterates
         * @param range indicates the range entry that the constructed iterator start iterating with
         */
        explicit iterator(iterable_group_store::values_type::iterator it) : it_(it) {}

        /**
         * @brief increment iterator
         * @return reference after the increment
         */
        iterator& operator++() {
            ++it_;
            return *this;
        }

        /**
         * @brief increment iterator
         * @return copy of the iterator before the increment
         */
        iterator operator++(int) {
            auto it = *this;
            this->operator++();
            return it;
        }

        /**
         * @brief dereference the iterator
         * @return record ref to the record that the iterator is on
         */
        [[nodiscard]] value_type operator*() {
            return ref();
        }

        /**
         * @brief dereference the iterator and return record ref
         * @return record ref to the record that the iterator is on
         */
        [[nodiscard]] value_type ref() const noexcept {
            return it_->ref();
        }

        /// @brief equivalent comparison
        bool operator==(iterator const& r) const noexcept {
            return this->it_ == r.it_;
        }

        /// @brief inequivalent comparison
        bool operator!=(const iterator& r) const noexcept {
            return !(*this == r);
        }

        /**
         * @brief appends string representation of the given value.
         * @param out the target output
         * @param value the target value
         * @return the output
         */
        friend inline std::ostream& operator<<(std::ostream& out, iterator value) {
            return out << std::hex
                << "original: [" << takatori::util::print_support(value.it_) <<"]";
        }

    private:
        iterable_group_store::values_type::iterator it_{};
    };

    [[nodiscard]] group_type::key_type const& key() const noexcept {
        return key_;
    }

    [[nodiscard]] group_type::value_groups const& values() const noexcept {
        return values_;
    }

    [[nodiscard]] iterator begin() noexcept {
        return iterator{values_.begin()};
    }

    [[nodiscard]] iterator end() noexcept {
        return iterator{values_.end()};
    }
private:
    group_type::key_type key_{};
    group_type::value_groups values_{};
};

}

