/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <vector>

#include <jogasaki/utils/iterator_pair.h>

namespace jogasaki::utils {

/**
 * @brief helper to increment a set of iterators
 * @details This is used to increment iterators as if each iterator is a digit. When the iterator at a position
 * reaches its end, carry-up occurs and the iterator next to it (at smaller position) increments.
 * @tparam Iterator the iterator type (only legacy input iterator requirement is assumed)
 */
template <class Iterator>
class iterator_incrementer {
public:
    using iterator = Iterator;
    using iterator_pair = utils::iterator_pair<iterator>;

    /**
     * @brief indicates the last position to increment
     */
    constexpr static std::size_t npos = static_cast<std::size_t>(-1);

    /**
     * @brief create new object
     * @param iterators the list of iterator pairs that define initial value (begin)
     * and upper bound (end) for each digit
     */
    explicit iterator_incrementer(std::vector<iterator_pair> iterators) noexcept :
        current_(std::move(iterators)),
        initial_(current_)
    {}

    /**
     * @brief increment the iterator at the specified position
     * @param pos the position to increment. When any value other than npos is specified, the increment affects only the digit on the position.
     * Specify npos to increment the last (least significant) digit and increment more significant digit when overflows.
     * @return true if the incremented iterators are valid
     * @return false otherwise. The result iterators are invalid and should not be used.
     */
    [[nodiscard]] bool increment(std::size_t pos = npos) {
        return increment_internal(pos, pos != npos);
    }

    /**
     * @brief accessor to the current iterators
     * @return list of iterators managed by this object
     */
    [[nodiscard]] std::vector<iterator_pair> const& current() const noexcept {
        return current_;
    }

    void reset(std::size_t pos = npos) {
        if(pos == npos) {
            current_ = initial_;
            return;
        }
        current_[pos] = initial_[pos];
    }
private:
    std::vector<iterator_pair> current_{};
    std::vector<iterator_pair> initial_{};

    [[nodiscard]] bool increment_internal(std::size_t pos, bool single_digit_only) {
        pos = (pos != npos) ? pos : (initial_.size()-1);
        if(! empty(current_[pos]) && ++current_[pos].first != current_[pos].second) {
            return true;
        }
        current_[pos].first = initial_[pos].first;
        if(pos == 0) {
            return false;
        }
        if (single_digit_only) {
            return false;
        }
        return increment_internal(pos-1, false);
    }
};

}
