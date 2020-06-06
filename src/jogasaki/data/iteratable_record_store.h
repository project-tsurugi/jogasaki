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


#include <array>
#include <vector>
#include <cstring>

#include <jogasaki/data/record_store.h>
#include <takatori/util/print_support.h>

namespace jogasaki::data {

/**
 * @brief record store with iterators
 * @details This container can store any number of records, which are backed by paged memory resource.
 * The stored records are accessible with the pointer-based iterator, which is pointer with custom increment operator
 * treats gaps between the pages (i.e. not all records are on the same continuous memory region, but iterator allow users
 * to iterate them sequentially as if they are continuous region)
 */
class iteratable_record_store {
public:
    /// @brief pointer type
    using record_pointer = record_store::record_pointer;

private:
    struct record_range {
        record_range(record_pointer b, record_pointer e) : b_(b), e_(e) {}
        record_pointer b_; //NOLINT
        record_pointer e_; //NOLINT
    };

public:
    /// @brief type for list of ranges
    using range_list = std::vector<record_range>;

    /**
     * @brief iterator for the stored records
     */
    class iterator {
    public:

        /// @brief iterator category
        using iterator_category = std::input_iterator_tag;

        /// @brief type of value
        using value_type = iteratable_record_store::record_pointer;

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
        iterator(iteratable_record_store const& container, range_list::iterator range) :
                container_(&container), pos_(range != container_->ranges_.end() ? range->b_ : nullptr), range_(range)
        {}

        /**
         * @brief increment iterator
         * @return reference after the increment
         */
        iterator& operator++() {
            pos_ = static_cast<unsigned char*>(pos_) + container_->record_size_; //NOLINT
            if (pos_ >= range_->e_) {
                ++range_;
                if(range_ != container_->ranges_.end()) {
                    pos_ = range_->b_;
                } else {
                    pos_ = nullptr;
                }
            }
            return *this;
        }

        /**
         * @brief increment iterator
         * @return copy of the iterator before the increment
         */
        iterator const operator++(int) { //NOLINT
            auto it = *this;
            this->operator++();
            return it;
        }

        /**
         * @brief dereference the iterator
         * @return pointer to the record that the iterator is on
         */
        reference operator*() {
            return pos_;
        }

        /// @brief equivalent comparison
        constexpr bool operator==(iterator const& r) const noexcept {
            return this->container_ == r.container_ && this->range_ == r.range_ && this->pos_ == r.pos_;
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
            return out << std::hex
                    << "container [" << value.container_
                    <<"] range [" << takatori::util::print_support(value.range_)
                    << "] pointer [" << value.pos_ << "]";
        }

    private:
        iteratable_record_store const* container_;
        iteratable_record_store::record_pointer pos_{};
        range_list::iterator range_;
    };

    /**
     * @brief create empty object
     */
    iteratable_record_store() = default;

    /**
     * @copydoc record_store::record_store(memory::paged_memory_resource* record_resource, memory::paged_memory_resource* varlen_resource, std::shared_ptr<meta::record_meta> meta)
     */
    iteratable_record_store(
            memory::paged_memory_resource* record_resource,
            memory::paged_memory_resource* varlen_resource,
            std::shared_ptr<meta::record_meta> meta) :
            record_size_(meta->record_size()),
            base_(record_resource, varlen_resource, std::move(meta))
    {}

    /**
     * @copydoc record_store::append()
     */
    record_pointer append(accessor::record_ref record) {
        auto p = base_.append(record);
        if (prev_ == nullptr || p != static_cast<unsigned char*>(prev_) + record_size_) { //NOLINT
            // starting new range
            ranges_.emplace_back(p, nullptr);
        }
        ranges_.back().e_ = static_cast<unsigned char*>(p) + record_size_; //NOLINT
        prev_ = p;
        return p;
    }

    /**
     * @copydoc record_store::count()
     */
    [[nodiscard]] std::size_t count() const noexcept {
        return base_.count();
    }

    /**
     * @copydoc record_store::empty()
     */
    [[nodiscard]] bool empty() const noexcept {
        return base_.empty();
    }

    /**
     * @brief getter of begin iterator
     * @return iterator at the beginning of the store
     * @warning the returned iterator will be invalid when new append() is called.
     */
    iterator begin() {
        return iterator{*this, ranges_.begin()};
    }

    /**
     * @brief getter of end iterator
     * @return iterator at the end of the store
     * @warning the returned iterator will be invalid when new append() is called
     */
    iterator end() {
        return iterator{*this, ranges_.end()};
    }

    /**
     * @copydoc record_store::reset()
     */
    void reset() noexcept {
        base_.reset();
        prev_ = nullptr;
        ranges_.clear();
    }

private:
    std::size_t record_size_{};
    record_store base_{};
    record_pointer prev_{};
    range_list ranges_{};

};

} // namespace
