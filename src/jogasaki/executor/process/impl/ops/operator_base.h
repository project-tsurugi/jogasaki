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

#include <takatori/relation/expression.h>
#include <jogasaki/executor/process/processor_info.h>
#include "operator_kind.h"

namespace jogasaki::executor::process::impl {
class block_scope_info;
}

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief relational operator base class
 */
class operator_base {
public:
    using block_index_type = std::size_t;

    static constexpr std::size_t npos = static_cast<std::size_t>(-1);
    /**
     * @brief create empty object
     */
    operator_base() = default;

    virtual ~operator_base() = default;

    operator_base(
        processor_info const& info,
        block_index_type block_index
    ) noexcept :
        processor_info_(std::addressof(info)),
        block_index_(block_index)
    {}

    operator_base(operator_base const& other) = default;
    operator_base& operator=(operator_base const& other) = default;
    operator_base(operator_base&& other) noexcept = default;
    operator_base& operator=(operator_base&& other) noexcept = default;

    [[nodiscard]] virtual operator_kind kind() const noexcept = 0;

    [[nodiscard]] block_scope_info const& block_info() const noexcept {
        return processor_info_->scopes_info()[block_index_];
    }

    [[nodiscard]] block_index_type block_index() const noexcept {
        return block_index_;
    }

    [[nodiscard]] std::vector<block_scope_info> const& blocks() const noexcept {
        return processor_info_->scopes_info();
    }

private:
    processor_info const* processor_info_{};
    block_index_type block_index_{};
};

}
