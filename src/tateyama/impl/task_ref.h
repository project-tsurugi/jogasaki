/*
 * Copyright 2018-2020 tsurugi project.
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

#include <cstdint>
#include <variant>
#include <ios>
#include <functional>

#include <glog/logging.h>
#include <tateyama/task.h>
#include <tateyama/impl/cache_align.h>

namespace tateyama::impl {

/**
 * @brief task context
 * @details dynamic context information of a task.
 */
class cache_align task_ref {
public:
    task_ref() = default;
    ~task_ref() = default;
    task_ref(task_ref const& other) = delete;
    task_ref& operator=(task_ref const& other) = delete;
    task_ref(task_ref&& other) noexcept = default;
    task_ref& operator=(task_ref&& other) noexcept = default;

    explicit task_ref(std::shared_ptr<task> t) :
        task_(std::move(t))
    {}

    [[nodiscard]] std::shared_ptr<task> const& body() const noexcept {
        return task_;
    }

private:
    std::shared_ptr<task> task_{};
};

static_assert(! std::is_copy_assignable_v<task_ref>);
static_assert(std::is_move_assignable_v<task_ref>);
static_assert(! std::is_copy_constructible_v<task_ref>);
static_assert(std::is_move_constructible_v<task_ref>);

}
