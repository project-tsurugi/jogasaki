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

#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>

#include <jogasaki/commit_profile.h>

namespace jogasaki::api::impl {

/**
 * @brief commit profiling statistics
 */
class commit_stats {
public:
    using time_point = commit_profile::time_point;

    /**
     * @brief construct empty object
     */
    commit_stats() = default;

    /**
     * @brief copy construct
     */
    commit_stats(commit_stats const&) = delete;

    /**
     * @brief move construct
     */
    commit_stats(commit_stats &&) = delete;

    /**
     * @brief copy assign
     */
    commit_stats& operator=(commit_stats const&) = delete;

    /**
     * @brief move assign
     */
    commit_stats& operator=(commit_stats &&) = delete;

    /**
     * @brief destruct record
     */
    ~commit_stats() = default;

    void add(commit_profile const& arg);
    void dump();
    void enabled(bool arg) noexcept;
    [[nodiscard]] bool enabled() const noexcept;

private:
    bool enabled_{false};
    std::atomic_size_t commits_requested_{};
    std::atomic_size_t precommit_cb_invoked_{};
    std::atomic_size_t durable_cb_invoked_{};
    std::atomic_size_t commit_job_completed_{};
    std::atomic_size_t sum_precommit_duration_ns_{};
    std::atomic_size_t sum_durability_duration_ns_{};
    std::atomic_size_t sum_notification_duration_ns_{};
    std::atomic_size_t min_precommit_duration_ns_{std::numeric_limits<std::size_t>::max()};
    std::atomic_size_t min_durability_duration_ns_{std::numeric_limits<std::size_t>::max()};
    std::atomic_size_t min_notification_duration_ns_{std::numeric_limits<std::size_t>::max()};
    std::atomic_size_t max_precommit_duration_ns_{};
    std::atomic_size_t max_durability_duration_ns_{};
    std::atomic_size_t max_notification_duration_ns_{};

    void update_min(std::atomic_size_t& target, std::size_t new_v);
    void update_max(std::atomic_size_t& target, std::size_t new_v);
};


}

