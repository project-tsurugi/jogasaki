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
#include <memory>

#include <jogasaki/commit_response.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/kvs/database.h>

namespace jogasaki {

struct commit_profile {
    using clock = std::chrono::steady_clock;
    using time_point = std::chrono::time_point<clock, std::chrono::nanoseconds>;

    void enabled(bool arg) noexcept {
        enabled_ = arg;
    }

    [[nodiscard]] bool enabled() const noexcept {
        return enabled_;
    }

    void set_commit_requested() noexcept {
        if(! enabled_) return;
        commit_requested_ = clock::now();
    }

    void set_precommit_cb_invoked() noexcept {
        if(! enabled_) return;
        precommit_cb_invoked_ = clock::now();
    }
    void set_durability_cb_invoked(time_point arg) noexcept {
        if(! enabled_) return;
        durability_cb_invoked_ = arg;
    }
    void set_commit_job_completed() noexcept {
        if(! enabled_) return;
        commit_job_completed_ = clock::now();
    }

    bool enabled_{};  //NOLINT
    time_point commit_requested_{};  //NOLINT
    time_point precommit_cb_invoked_{};  //NOLINT
    time_point durability_cb_invoked_{};  //NOLINT
    time_point commit_job_completed_{};  //NOLINT
};

}

