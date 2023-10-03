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
#include <iomanip>

#include <tateyama/task_scheduler/schedule_option.h>

namespace jogasaki::scheduler {

/**
 * @brief schedule policy kind
 * @details this kind defines the worker selection on task schedule
 */
enum class schedule_policy_kind : std::size_t {
    /**
     * @brief policy undefined (default)
     * @details try to use preferred worker for current thread (if config. option allows) or round-robbin workers
     */
    undefined = 0,

    /**
     * @brief policy to use suspended worker first
     * @details find suspended worker and schedule to it. If not found, fall-back to `undefined`.
     */
    suspended_worker,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(schedule_policy_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = schedule_policy_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::suspended_worker: return "suspended_worker"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, schedule_policy_kind value) {
    return out << to_string_view(value);
}

/**
 * @brief task scheduler scheduling option
 */
class schedule_option {
public:
    /**
     * @brief create default object
     */
    schedule_option() = default;

    /**
     * @brief destruct the object
     */
    ~schedule_option() = default;

    schedule_option(schedule_option const& other) = default;
    schedule_option& operator=(schedule_option const& other) = default;
    schedule_option(schedule_option&& other) noexcept = default;
    schedule_option& operator=(schedule_option&& other) noexcept = default;

    /**
     * @brief create new object with given policy
     */
    explicit schedule_option(schedule_policy_kind policy) noexcept :
        policy_(policy)
    {}

    [[nodiscard]] schedule_policy_kind policy() const noexcept {
        return policy_;
    }

private:
    schedule_policy_kind policy_{};
};

inline tateyama::task_scheduler::schedule_option convert(schedule_option opt) {
    tateyama::task_scheduler::schedule_policy_kind policy{};
    switch(opt.policy()) {
        case schedule_policy_kind::undefined: policy = tateyama::task_scheduler::schedule_policy_kind::undefined; break;
        case schedule_policy_kind::suspended_worker: policy = tateyama::task_scheduler::schedule_policy_kind::suspended_worker; break;
    }
    return tateyama::task_scheduler::schedule_option{policy};
}


}

