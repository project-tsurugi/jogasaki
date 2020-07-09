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

#include "operator_kind.h"

namespace jogasaki::executor::process::impl::relop {

/**
 * @brief relational operator base class
 */
class operator_base {
public:
    /**
     * @brief create empty object
     */
    operator_base() = default;

    virtual ~operator_base() = default;

    virtual operator_kind kind() = 0;

    [[nodiscard]] std::size_t const& block_index() const noexcept {
        return block_index_;
    }

    void block_index(std::size_t index) {
        block_index_ = index;
    }
private:
    std::size_t block_index_{};
};

}


