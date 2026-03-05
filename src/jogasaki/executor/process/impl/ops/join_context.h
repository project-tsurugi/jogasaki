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

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;

/**
 * @brief join context
 */
template <class Iterator>
class join_context : public context_base {
public:
    template <class T>
    friend class join;

    using iterator = Iterator;

    using iterator_incrementer = utils::iterator_incrementer<iterator>;

    /**
     * @brief create empty object
     */
    join_context() = default;

    /**
     * @brief create new object
     */
    join_context(
        class abstract::task_context* ctx,
        variable_table& variables,
        memory_resource* resource = nullptr,
        memory_resource* varlen_resource = nullptr
    ) :
        context_base(ctx, variables, resource, varlen_resource)
    {}

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::join;
    }

    void release() override {
        //no-op
    }

    // frame variables for yield
    iterator_incrementer incr_;  //NOLINT
    bool exists_match_{};  //NOLINT
    bool secondary_group_available_{};  //NOLINT
    std::size_t secondary_group_pos_{};  //NOLINT
    boost::dynamic_bitset<std::uint64_t> unmatched_right_{};  //NOLINT
    std::size_t right_group_size_{};  //NOLINT
    std::size_t idx_{};  //NOLINT
    bool resuming_calling_child_6_{};  //NOLINT
};

}


