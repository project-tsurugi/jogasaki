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

#include <cstddef>

namespace jogasaki::scheduler {

class thread_info {
public:
    static constexpr std::size_t undefined = static_cast<std::size_t>(-1);

    thread_info() = default;

    thread_info(
        bool is_worker_thread,
        std::size_t worker_index
    ) :
        is_worker_thread_(is_worker_thread),
        worker_index_(worker_index)
    {}

    [[nodiscard]] bool is_worker_thread() const noexcept {
        return is_worker_thread_;
    }

    [[nodiscard]] std::size_t worker_index() const noexcept {
        return worker_index_;
    }

private:
    bool is_worker_thread_{};
    std::size_t worker_index_{undefined};
};

} // namespace
