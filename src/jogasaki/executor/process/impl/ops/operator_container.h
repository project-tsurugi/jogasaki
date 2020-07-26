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
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/block_scope_info.h>
#include <jogasaki/executor/process/impl/ops/process_io_map.h>

namespace jogasaki::executor::process::impl::ops {

namespace relation = takatori::relation;

/**
 * @brief relational operators container
 */
class operator_container {
public:
    using operators_type = std::unordered_map<relation::expression const*, std::unique_ptr<ops::operator_base>>;

    operator_container() = default;

    explicit operator_container(
        operators_type operators,
        process_io_map io_map
    ) :
        operators_(std::move(operators)),
        io_map_(std::move(io_map))
    {}

    [[nodiscard]] std::size_t count(relation::expression const* op) const noexcept {
        return operators_.count(op);
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return operators_.size();
    }

    [[nodiscard]] ops::operator_base* at(relation::expression const* op) const noexcept {
        return operators_.at(op).get();
    }

    auto begin() const noexcept {
        return operators_.begin();
    }

    auto end() const noexcept {
        return operators_.end();
    }

    [[nodiscard]] process_io_map const& io_map() const noexcept {
        return io_map_;
    };

private:
    operators_type operators_{};
    process_io_map io_map_{};
};

}

