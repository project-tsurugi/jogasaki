/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <atomic>
#include <cstddef>

namespace plugin::udf {

class round_robin_selector {
  public:
    round_robin_selector() = default;
    round_robin_selector(round_robin_selector const&) = delete;
    round_robin_selector& operator=(round_robin_selector const&) = delete;
    round_robin_selector(round_robin_selector&&) = delete;
    round_robin_selector& operator=(round_robin_selector&&) = delete;
    ~round_robin_selector() = default;

    [[nodiscard]] std::size_t next(std::size_t size) noexcept {
        if (size <= 1) { return 0; }
        return next_.fetch_add(1, std::memory_order_relaxed) % size;
    }

    /**
     * @brief select next index skipping candidates for which `available(index)` is false
     * @details scanning starts at the current round robin position and the position advances to
     * the one past the selected index, so that skipped candidates do not skew the distribution
     * over the remaining ones. If no candidate is available, the start position itself is
     * returned so that the caller can still issue a request (and receive the error) while the
     * position advances past it.
     * @tparam Predicate callable `bool(std::size_t index)` returning whether the index is usable
     * @param size number of candidates
     * @param available predicate to check availability of a candidate (may block, and is invoked
     * at most `size` times)
     * @return the selected index in [0, size)
     */
    template <class Predicate>
    [[nodiscard]] std::size_t next(std::size_t size, Predicate const& available) {
        if (size <= 1) { return 0; }
        auto expected = next_.load(std::memory_order_relaxed);
        auto const start = expected % size;
        auto selected = start;
        for (std::size_t i = 0; i < size; ++i) {
            auto const idx = (start + i) % size;
            if (available(idx)) {
                selected = idx;
                break;
            }
        }
        // advance the position past the selected index. On concurrent updates keep the other
        // thread's position rather than re-scanning with the (possibly blocking) predicate.
        next_.compare_exchange_strong(expected, selected + 1, std::memory_order_relaxed);
        return selected;
    }

  private:
    std::atomic_size_t next_{0};
};

} // namespace plugin::udf
