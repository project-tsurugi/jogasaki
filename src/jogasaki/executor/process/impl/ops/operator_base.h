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

    using operator_index_type = std::size_t;

    static constexpr std::size_t npos = static_cast<std::size_t>(-1);
    /**
     * @brief create empty object
     */
    operator_base() = default;

    virtual ~operator_base() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     */
    operator_base(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index
    ) noexcept;

    operator_base(operator_base const& other) = default;
    operator_base& operator=(operator_base const& other) = default;
    operator_base(operator_base&& other) noexcept = default;
    operator_base& operator=(operator_base&& other) noexcept = default;

    [[nodiscard]] virtual operator_kind kind() const noexcept = 0;

    [[nodiscard]] block_scope_info const& block_info() const noexcept;

    [[nodiscard]] block_index_type block_index() const noexcept;

    [[nodiscard]] std::vector<block_scope_info> const& blocks() const noexcept;

    [[nodiscard]] yugawara::compiled_info const& compiled_info() const noexcept;

    [[nodiscard]] operator_index_type index() const noexcept;
private:
    operator_index_type index_{};
    processor_info const* processor_info_{};
    block_index_type block_index_{};

};

class context_helper;

class record_operator : public operator_base {
public:
    record_operator() = default;

    record_operator(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index
    ) noexcept;

    virtual void process_record(context_helper* parent) = 0;
};

class group_operator : public operator_base {
public:
    group_operator() = default;

    group_operator(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index
    ) noexcept;

    virtual void process_group(context_helper* parent, bool first) = 0;
};

}
