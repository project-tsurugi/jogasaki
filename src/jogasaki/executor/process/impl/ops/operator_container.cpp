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
#include "operator_container.h"

#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/executor/process/impl/scan_info.h>

namespace jogasaki::executor::process::impl::ops {


operator_container::operator_container(std::unique_ptr<ops::operator_base> root, std::size_t operator_count,
    class io_exchange_map& io_exchange_map, std::shared_ptr<impl::scan_info> scan_info) :
    root_(std::move(root)),
    operator_count_(operator_count),
    io_exchange_map_(std::addressof(io_exchange_map)),
    scan_info_(std::move(scan_info))
{}

std::size_t operator_container::size() const noexcept {
    return operator_count_;
}

class io_exchange_map const& operator_container::io_exchange_map() const noexcept {
    return *io_exchange_map_;
}

operator_base& operator_container::root() const noexcept {
    return *root_;
}

std::shared_ptr<impl::scan_info> const& operator_container::scan_info() const noexcept {
    return scan_info_;
}
}

