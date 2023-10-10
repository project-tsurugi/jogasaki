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

#include <memory>
#include "../../third_party/nlohmann/json.hpp"

#include <jogasaki/logging_helper.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::api::impl {

/**
 * @brief error info object
 * @details this object represents the error information of the API request
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

    void add(commit_profile const& arg) {
        if(arg.commit_requested_ != time_point{}) {
            ++commits_requested_;
        }
        if(arg.precommit_cb_invoked_ != time_point{}) {
            ++precommit_cb_invoked_;
            precommit_duration_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(arg.precommit_cb_invoked_ - arg.commit_requested_).count();
        }
        if(arg.durability_cb_invoked_ != time_point{}) {
            ++durable_cb_invoked_;
            durability_duration_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(arg.durability_cb_invoked_ - arg.precommit_cb_invoked_).count();
        }
        if(arg.commit_job_completed_ != time_point{}) {
            ++commit_job_completed_;
            if(arg.durability_cb_invoked_ != time_point{}) {
                notification_duration_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(arg.commit_job_completed_ - arg.durability_cb_invoked_).count();
            } else if(arg.precommit_cb_invoked_ != time_point{}) {
                notification_duration_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(arg.commit_job_completed_ - arg.precommit_cb_invoked_).count();
            }
        }
    }

    void dump() {
        using json = nlohmann::json;
        try {
            json j{};
            j["commits_requested"] = commits_requested_.load();
            j["precommit_cb_invoked"] = precommit_cb_invoked_.load();
            j["durable_cb_invoked"] = durable_cb_invoked_.load();
            j["commit_job_completed"] = commit_job_completed_.load();
            j["precommit_duration_ns"] = precommit_cb_invoked_.load() ? (precommit_duration_ns_.load() / precommit_cb_invoked_.load()) : 0;
            j["durability_duration_ns"] = durable_cb_invoked_.load() ? (durability_duration_ns_.load() / durable_cb_invoked_.load()) : 0;
            j["notification_duration_ns"] = commit_job_completed_.load() ? (notification_duration_ns_.load() / commit_job_completed_.load()) : 0;
            LOG_LP(INFO) << "commit_profile " << j.dump();
        } catch (json::exception const& e) {
            LOG_LP(INFO) << "json exception " << e.what();
        }
    }
private:
    std::atomic_size_t commits_requested_{};
    std::atomic_size_t precommit_cb_invoked_{};
    std::atomic_size_t durable_cb_invoked_{};
    std::atomic_size_t commit_job_completed_{};
    std::atomic_size_t precommit_duration_ns_{};
    std::atomic_size_t durability_duration_ns_{};
    std::atomic_size_t notification_duration_ns_{};
};


}

