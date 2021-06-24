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
#include <tateyama/impl/thread_control.h>
#include <tateyama/impl/cache_align.h>

namespace tateyama {

/**
 * @brief worker context information
 * @details dynamic context information of the worker
 */
class cache_align context {
public:
    context() = default;
    ~context() = default;
    context(context const& other) = default;
    context& operator=(context const& other) = default;
    context(context&& other) noexcept = default;
    context& operator=(context&& other) noexcept = default;

    /**
     * @brief create new context object
     * @param index the worker index
     */
    explicit context(std::size_t index) noexcept :
        index_(index)
    {}

    /**
     * @brief accessor to the worker index
     * @return the 0-origin index of the worker, that is associated with this context
     */
    [[nodiscard]] std::size_t index() const noexcept {
        return index_;
    }

private:
    std::size_t index_{};
};

}
