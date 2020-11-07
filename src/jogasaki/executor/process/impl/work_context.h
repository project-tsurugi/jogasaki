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

#include <memory>
#include <jogasaki/executor/process/abstract/work_context.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/block_scope.h>

namespace jogasaki::executor::process::impl {

/**
 * @brief processor working context implementation for production
 */
class work_context : public process::abstract::work_context {
public:
    using block_scopes = std::vector<block_scope>;
    using memory_resource = ops::context_base::memory_resource;

    work_context() = default;

    explicit work_context(std::size_t operator_count) : contexts_(operator_count) {}

    work_context(
        ops::context_container contexts,
        block_scopes variables,
        std::unique_ptr<memory_resource> resource
    ) :
    //TODO fix move semantics
        contexts_(std::move(contexts)),
        variables_(std::move(variables)),
        resource_(std::move(resource))
    {}

    ~work_context() override {
        contexts_.release();
    }

    work_context(work_context const& other) = delete;
    work_context& operator=(work_context const& other) = delete;
    work_context(work_context&& other) noexcept = delete;
    work_context& operator=(work_context&& other) noexcept = delete;

    [[nodiscard]] ops::context_container& container() noexcept {
        return contexts_;
    }

    [[nodiscard]] block_scopes& scopes() noexcept {
        return variables_;
    }

    [[nodiscard]] block_scope& variables(std::size_t block_index) noexcept {
        return variables_[block_index];
    }

    [[nodiscard]] memory_resource* resource() const noexcept {
        return resource_.get();
    }

private:
    ops::context_container contexts_{};
    block_scopes variables_{};
    std::unique_ptr<memory_resource> resource_{};
};

}


