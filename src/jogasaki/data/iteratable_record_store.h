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
    using pointer = record_store::pointer;

    struct pointers_interval {
        pointers_interval(pointer b, pointer e) : b_(b), e_(e) {}
        pointer b_; //NOLINT
        pointer e_; //NOLINT
    };

    using interval_list = std::vector<pointers_interval>;

    class iteratable_record_store_iterator {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = iteratable_record_store::pointer;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;
        iteratable_record_store_iterator(iteratable_record_store const& container, interval_list::iterator interval) :
                container_(&container), pos_(interval != container_->intervals_.end() ? interval->b_ : nullptr), interval_(interval)
        {}

        iteratable_record_store_iterator& operator++() {
            pos_ = static_cast<unsigned char*>(pos_) + container_->record_size_; //NOLINT
            if (pos_ >= interval_->e_) {
                ++interval_;
                if(interval_ != container_->intervals_.end()) {
                    pos_ = interval_->b_;
                } else {
                    pos_ = nullptr;
                }
            }
            return *this;
        }

        iteratable_record_store_iterator const operator++(int) { //NOLINT
            auto it = *this;
            this->operator++();
            return it;
        }

        reference operator*() {
            return pos_;
        }

        value_type operator->() {
            return pos_;
        }

        constexpr bool operator==(iteratable_record_store_iterator const& r) const noexcept {
            return this->container_ == r.container_ && this->interval_ == r.interval_ && this->pos_ == r.pos_;
        }

        constexpr bool operator!=(const iteratable_record_store_iterator& r) const noexcept {
            return !(*this == r);
        }

        /**
         * @brief appends string representation of the given value.
         * @param out the target output
         * @param value the target value
         * @return the output
         */
        friend inline std::ostream& operator<<(std::ostream& out, iteratable_record_store_iterator value) {
            return out << std::hex
                    << "container [" << value.container_
                    <<"] interval [" << takatori::util::print_support(value.interval_)
                    << "] pointer [" << value.pos_ << "]";
        }

    private:
        iteratable_record_store const* container_;
        iteratable_record_store::pointer pos_{};
        interval_list::iterator interval_;
    };

    using iterator = iteratable_record_store_iterator;

    /**
     * @brief create empty object
     */
    iteratable_record_store() = default;

    /**
     * @copydoc record_store::record_state()
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
    pointer append(accessor::record_ref record) {
        auto p = base_.append(record);
        if (prev_ == nullptr || p != static_cast<unsigned char*>(prev_) + record_size_) { //NOLINT
            // starting new interval
            intervals_.emplace_back(p, nullptr);
        }
        intervals_.back().e_ = static_cast<unsigned char*>(p) + record_size_; //NOLINT
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
        return iterator{*this, intervals_.begin()};
    }

    /**
     * @brief getter of end iterator
     * @return iterator at the end of the store
     * @warning the returned iterator will be invalid when new append() is called
     */
    iterator end() {
        return iterator{*this, intervals_.end()};
    }

    /**
     * @copydoc record_store::reset()
     */
    void reset() noexcept {
        base_.reset();
        prev_ = nullptr;
        intervals_.clear();
    }

private:
    std::size_t record_size_{};
    record_store base_{};
    pointer prev_{};
    interval_list intervals_{};

};

} // namespace
