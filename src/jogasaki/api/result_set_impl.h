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

#include <jogasaki/executor/global.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::api {

/**
 * @brief iterator for result set
 */
class result_set::iterator {
public:

    /// @brief iterator category
    using iterator_category = std::input_iterator_tag;

    /// @brief type of value
    using value_type = data::iterable_record_store::value_type;

    /// @brief type of difference
    using difference_type = std::ptrdiff_t;

    /// @brief type of pointer
    using pointer = value_type*;

    /// @brief type of reference
    using reference = value_type&;

    using original_iterator = data::iterable_record_store::iterator;

    /**
     * @brief construct new iterator
     * @param container the target record store that the constructed object iterates
     * @param range indicates the range entry that the constructed iterator start iterating with
     */
    iterator(
        original_iterator it,
        data::result_store& store,
        std::size_t partition
    ) :
        it_(it),
        store_(std::addressof(store)),
        partition_(partition)
    {
        proceed_if_absent();
    }

    /**
     * @brief increment iterator
     * @return reference after the increment
     */
    iterator& operator++() {
        ++it_;
        proceed_if_absent();
        return *this;
    }

    /**
     * @brief increment iterator
     * @return copy of the iterator before the increment
     */
    iterator const operator++(int) {
        iterator cur{*this};
        ++it_;
        return cur;
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
    [[nodiscard]] accessor::record_ref ref() const noexcept {
        return it_.ref();
    }

    /// @brief equivalent comparison
    constexpr bool operator==(iterator const& r) const noexcept {
        return it_ == r.it_;
    }

    /// @brief inequivalent comparison
    constexpr bool operator!=(const iterator& r) const noexcept {
        return !(*this == r);
    }

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend inline std::ostream& operator<<(std::ostream& out, iterator value) {
        return out << value.it_;
    }

private:
    original_iterator it_;
    data::result_store* store_{};
    std::size_t partition_{};

    void proceed_if_absent() {
        while (it_ == store_->store(partition_).end() && partition_ != store_->size()-1) {
            ++partition_;
            it_ = store_->store(partition_).begin();
        }
    }
};

class result_set::impl {
public:
    explicit impl(
        std::unique_ptr<data::result_store> store
    ) noexcept :
        store_(std::move(store))
    {}

    [[nodiscard]] maybe_shared_ptr<meta::record_meta> meta() const noexcept;

    [[nodiscard]] iterator begin();
    [[nodiscard]] iterator end();
    void close();

private:
    std::unique_ptr<data::result_store> store_{};
};

}
