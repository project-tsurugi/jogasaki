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

#include <cstddef>
#include <memory>

#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/scan_info.h>
#include <jogasaki/executor/process/io_exchange_map.h>

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief relational operators container
 */
class operator_container {
public:
    /**
     * @brief create empty object
     */
    operator_container() = default;

    /**
     * @brief create new object
     * @param root the root of the operator tree
     * @param operator_count the number of operators
     * @param io_exchange_map the mapping from input/output index to exchange
     * @param scan_info the scan information gathered from the scan operator in the operator tree (if any).
     * Can be nullptr if the operators don't contain scan operation.
     */
    operator_container(
        std::unique_ptr<ops::operator_base> root,
        std::size_t operator_count,
        class io_exchange_map& io_exchange_map,
        std::shared_ptr<impl::scan_info> scan_info
    );

    /**
     * @brief accessor to operator count
     * @return the number of operators
     */
    [[nodiscard]] std::size_t size() const noexcept;

    /**
     * @brief accessor to I/O exchange mapping
     * @return the mapping object
     */
    [[nodiscard]] class io_exchange_map const& io_exchange_map() const noexcept;;

    /**
     * @brief accessor to operator tree root
     * @return the root object of the operators
     */
    [[nodiscard]] ops::operator_base& root() const noexcept;

    /**
     * @brief accessor to scan info
     * @return the scan info, or nullptr if there is no scan operation in the process
     */
    [[nodiscard]] std::shared_ptr<impl::scan_info> const& scan_info() const noexcept;

private:
    std::unique_ptr<ops::operator_base> root_{};
    std::size_t operator_count_{};
    class io_exchange_map* io_exchange_map_{};
    std::shared_ptr<impl::scan_info> scan_info_{};
};

}

