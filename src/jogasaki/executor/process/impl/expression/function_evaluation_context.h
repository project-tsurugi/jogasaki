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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>

namespace jogasaki::executor::process::impl::expression {

/**
 * @brief object to hold transaction (begin) timestamp
 */
class function_evaluation_context {
public:
    using clock = std::chrono::steady_clock;

    function_evaluation_context() = default;

    void transaction_begin(std::optional<clock::time_point> arg) noexcept {
        transaction_begin_ = arg;
    }

    [[nodiscard]] std::optional<clock::time_point> transaction_begin() const noexcept {
        return transaction_begin_;
    }

private:
    std::optional<clock::time_point> transaction_begin_{};
};

}  // namespace jogasaki
