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

#include <takatori/util/downcast.h>

#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/abstract/task_context.h>

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

/**
 * @brief context access helper
 * @details a wrapper to task context that helps extracting objects
 */
class context_helper {
public:
    using memory_resource = context_base::memory_resource;

    ~context_helper() = default;
    context_helper(context_helper const& other) = delete;
    context_helper& operator=(context_helper const& other) = delete;
    context_helper(context_helper&& other) noexcept = delete;
    context_helper& operator=(context_helper&& other) noexcept = delete;

    /**
     * @brief create helper object for the task context
     * @param context the original task context
     */
    explicit context_helper(
        abstract::task_context &context
    ) noexcept;

    /**
     * @brief make operator context and store it in the context list held by task context (work context)
     * @tparam T the operator context type
     * @tparam Args arguments types to construct operator context
     * @param index the index to position the context in the context list
     * @param args arguments to construct operator context
     * @return pointer to the created/stored context
     */
    template<class T, class ... Args>
    [[nodiscard]] T* make_context(std::size_t index, Args&&...args) {
        auto& p = contexts().set(index, std::make_unique<T>(context_, std::forward<Args>(args)...));
        return unsafe_downcast<T>(p.get());
    }

    /**
     * @brief accessor to context_container
     */
    [[nodiscard]] context_container& contexts() const noexcept;

    /**
     * @brief accessor to memory resource for work area
     */
    [[nodiscard]] memory_resource* resource() const noexcept;

    /**
     * @brief accessor to memory resource for work area
     */
    [[nodiscard]] memory_resource* varlen_resource() const noexcept;

    /**
     * @brief accessor to kvs database
     */
    [[nodiscard]] kvs::database* database() const noexcept;

    /**
     * @brief accessor to kvs transaction
     */
    [[nodiscard]] kvs::transaction* transaction() const noexcept;

    /**
     * @brief accessor to block_scope
     */
    [[nodiscard]] class block_scope& block_scope(std::size_t index);

    /**
     * @brief accessor to task context
     */
    [[nodiscard]] abstract::task_context* task_context() const noexcept;

private:
    abstract::task_context *context_{};
    impl::work_context* work_context_{};
};

}


