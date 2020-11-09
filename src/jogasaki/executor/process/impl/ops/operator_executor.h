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

#include <glog/logging.h>

#include <jogasaki/executor/process/impl/ops/operator_container.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/abstract/task_context.h>

namespace jogasaki::executor::process::impl::ops {

class operator_executor {
public:
    using memory_resource = context_base::memory_resource;

    operator_executor() = default;
    ~operator_executor() = default;
    operator_executor(operator_executor const& other) = delete;
    operator_executor& operator=(operator_executor const& other) = delete;
    operator_executor(operator_executor&& other) noexcept = delete;
    operator_executor& operator=(operator_executor&& other) noexcept = delete;

    operator_executor(
        operator_container* operators,
        abstract::task_context *context,
        memory_resource* resource,
        kvs::database* database
    ) noexcept;

    template<class T, class ... Args>
    [[nodiscard]] T* make_context(std::size_t index, Args&&...args) {
        auto& container = static_cast<work_context *>(context_->work_context())->container();  //NOLINT
        auto& p = container.set(index, std::make_unique<T>(context_, std::forward<Args>(args)...));
        return static_cast<T*>(p.get());
    }

    void operator()();

    [[nodiscard]] operator_container& operators() const noexcept;

    [[nodiscard]] context_container& contexts() const noexcept;

    [[nodiscard]] memory_resource* resource() const noexcept;

    [[nodiscard]] kvs::database* database() const noexcept;

    [[nodiscard]] block_scope& get_block_variables(std::size_t index);

    [[nodiscard]] abstract::task_context* task_context() const noexcept;

private:
    operator_container* operators_{};
    abstract::task_context *context_{};
    memory_resource* resource_{};
    kvs::database* database_{};
    operator_base* root_{};

};

}


