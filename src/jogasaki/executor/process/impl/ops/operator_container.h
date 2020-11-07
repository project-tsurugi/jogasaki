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
#include <jogasaki/executor/process/impl/details/io_exchange_map.h>
#include <jogasaki/executor/process/impl/scan_info.h>

namespace jogasaki::executor::process::impl::ops {

namespace relation = takatori::relation;

/**
 * @brief relational operators container
 */
class operator_container {
public:
    operator_container() = default;

    explicit operator_container(
        std::unique_ptr<ops::operator_base> root,
        std::size_t operator_count,
        details::io_exchange_map io_exchange_map,
        std::shared_ptr<impl::scan_info> scan_info
    ) :
        root_(std::move(root)),
        operator_count_(operator_count),
        io_exchange_map_(std::move(io_exchange_map)),
        scan_info_(std::move(scan_info))
    {}

    [[nodiscard]] std::size_t size() const noexcept {
        return operator_count_;
    }

    [[nodiscard]] details::io_exchange_map const& io_exchange_map() const noexcept {
        return io_exchange_map_;
    };

    [[nodiscard]] ops::operator_base& root() const noexcept {
        return *root_;
    }

    [[nodiscard]] std::shared_ptr<impl::scan_info> const& scan_info() const noexcept {
        return scan_info_;
    }
private:
    std::unique_ptr<ops::operator_base> root_{};
    std::size_t operator_count_{};
    details::io_exchange_map io_exchange_map_{};
    std::shared_ptr<impl::scan_info> scan_info_{};
};

}

