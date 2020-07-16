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
#include <jogasaki/executor/process/impl/relop/operator_base.h>
#include <jogasaki/executor/process/impl/block_variables_info.h>

namespace jogasaki::executor::process::impl::relop {

namespace relation = takatori::relation;

/**
 * @brief relational operators container
 */
class operator_container {
public:
    using operators_type = std::unordered_map<relation::expression const*, std::unique_ptr<relop::operator_base>>;

    operator_container() = default;

    explicit operator_container(
        operators_type operators
    ) :
        operators_(std::move(operators))
    {}

    void set_block_index(blocks_index_type const& indices) {
        for(auto& [e, o] : operators_) {
            o->block_index(indices.at(e));
        }
    }
    [[nodiscard]] std::size_t count(relation::expression const* op) const noexcept {
        return operators_.count(op);
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return operators_.size();
    }

    [[nodiscard]] relop::operator_base* at(relation::expression const* op) const noexcept {
        return operators_.at(op).get();
    }

    auto begin() const noexcept {
        return operators_.begin();
    }

    auto end() const noexcept {
        return operators_.end();
    }
private:
    operators_type operators_{};
};

}

