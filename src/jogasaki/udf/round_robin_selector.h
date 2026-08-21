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

  private:
    std::atomic_size_t next_{0};
};

} // namespace plugin::udf
