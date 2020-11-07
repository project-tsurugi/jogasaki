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

#include <unordered_map>

#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/context_base.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief relational operator context container
 */
class cache_align context_container {
public:
    using contexts_type = std::vector<std::unique_ptr<ops::context_base>>;

    context_container() = default;

    explicit context_container(
        std::size_t size
    ) :
        contexts_(size)
    {}

    template <typename ... Args>
    auto emplace(Args&& ... args) {
        return contexts_.emplace(std::forward<Args>(args)...);
    }

    std::unique_ptr<context_base>& set(std::size_t idx, std::unique_ptr<context_base> ctx) noexcept {
        if (idx >= contexts_.size()) fail();
        contexts_[idx] = std::move(ctx);
        return contexts_[idx];
    }

    [[nodiscard]] bool exists(std::size_t idx) const noexcept {
        return contexts_[idx] != nullptr;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return contexts_.size();
    }

    [[nodiscard]] ops::context_base* at(std::size_t idx) const noexcept {
        if (idx >= contexts_.size()) return nullptr;
        return contexts_.at(idx).get();
    }

    void release() {
        for(auto&& op : contexts_) {
            op.reset();
        }
    }
private:
    contexts_type contexts_{};
};

template<class T>
[[nodiscard]] T* find_context(std::size_t idx, context_container& container) {
    return static_cast<T*>(container.at(idx));
}

}

