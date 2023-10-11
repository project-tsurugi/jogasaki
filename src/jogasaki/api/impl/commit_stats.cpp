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
#include "commit_stats.h"

#include <memory>
#include "../../third_party/nlohmann/json.hpp"

#include <jogasaki/logging_helper.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::api::impl {

void commit_stats::dump() {
    if(! enabled_) return;
    using json = nlohmann::json;
    try {
        json j{};
        j["count_commits_requested"] = commits_requested_.load();
        j["count_precommit_cb_invoked"] = precommit_cb_invoked_.load();
        j["count_durable_cb_invoked"] = durable_cb_invoked_.load();
        j["count_commit_job_completed"] = commit_job_completed_.load();

        j["min_duration_ns_precommit"] = min_precommit_duration_ns_.load();
        j["max_duration_ns_precommit"] = max_precommit_duration_ns_.load();
        j["avg_duration_ns_precommit"] = precommit_cb_invoked_.load() != 0 ? (sum_precommit_duration_ns_.load() / precommit_cb_invoked_.load()) : 0;

        j["min_duration_ns_durability"] = min_durability_duration_ns_.load();
        j["max_duration_ns_durability"] = max_durability_duration_ns_.load();
        j["avg_duration_ns_durability"] = durable_cb_invoked_.load() != 0 ? (sum_durability_duration_ns_.load() / durable_cb_invoked_.load()) : 0;

        j["min_duration_ns_notification"] = min_notification_duration_ns_.load();
        j["max_duration_ns_notification"] = max_notification_duration_ns_.load();
        j["avg_duration_ns_notification"] = commit_job_completed_.load() != 0 ? (sum_notification_duration_ns_.load() / commit_job_completed_.load()) : 0;
        LOG_LP(INFO) << "commit_profile " << j.dump();
    } catch (json::exception const& e) {
        LOG_LP(INFO) << "json exception " << e.what();
    }
}

void commit_stats::add(commit_profile const& arg) {
    if(! enabled_) return;
    if(arg.commit_requested_ != time_point{}) {
        ++commits_requested_;
    }
    if(arg.precommit_cb_invoked_ != time_point{}) {
        ++precommit_cb_invoked_;
        auto e = std::chrono::duration_cast<std::chrono::nanoseconds>(arg.precommit_cb_invoked_ - arg.commit_requested_).count();
        sum_precommit_duration_ns_ += e;
        update_min(min_precommit_duration_ns_, e);
        update_max(max_precommit_duration_ns_, e);
    }
    if(arg.durability_cb_invoked_ != time_point{}) {
        ++durable_cb_invoked_;
        auto e = std::chrono::duration_cast<std::chrono::nanoseconds>(arg.durability_cb_invoked_ - arg.precommit_cb_invoked_).count();
        sum_durability_duration_ns_ += e;
        update_min(min_durability_duration_ns_, e);
        update_max(max_durability_duration_ns_, e);
    }
    if(arg.commit_job_completed_ != time_point{}) {
        ++commit_job_completed_;
        std::size_t e{};
        if(arg.durability_cb_invoked_ != time_point{}) {
            e = std::chrono::duration_cast<std::chrono::nanoseconds>(arg.commit_job_completed_ - arg.durability_cb_invoked_).count();
        } else if(arg.precommit_cb_invoked_ != time_point{}) {
            e = std::chrono::duration_cast<std::chrono::nanoseconds>(arg.commit_job_completed_ - arg.precommit_cb_invoked_).count();
        }
        sum_notification_duration_ns_ += e;
        update_min(min_notification_duration_ns_, e);
        update_max(max_notification_duration_ns_, e);
    }
}

void commit_stats::update_min(std::atomic_size_t& target, std::size_t new_v) {
    auto cur = target.load();
    do {
        if(new_v >= cur) {
            return;
        }
    } while(target.compare_exchange_strong(cur, new_v));
}

void commit_stats::update_max(std::atomic_size_t& target, std::size_t new_v) {
    auto cur = target.load();
    do {
        if(new_v <= cur) {
            return;
        }
    } while(target.compare_exchange_strong(cur, new_v));
}

void commit_stats::enabled(bool arg) noexcept {
    enabled_ = arg;
}

bool commit_stats::enabled() const noexcept {
    return enabled_;
}
}

