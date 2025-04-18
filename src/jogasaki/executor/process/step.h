/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <takatori/util/sequence_view.h>

#include <jogasaki/constants.h>
#include <jogasaki/executor/common/step.h>
#include <jogasaki/executor/process/abstract/process_executor.h>
#include <jogasaki/executor/process/impl/ops/io_info.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/model/step.h>
#include <jogasaki/model/step_kind.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>

#include "flow.h"

namespace jogasaki::executor::process {

using jogasaki::executor::process::impl::ops::io_info;

class step : public common::step {
public:
    step() = default;

    step(
        std::shared_ptr<processor_info> info,
        std::shared_ptr<class relation_io_map> relation_io_map,
        std::shared_ptr<class io_info> io_info = {}
    );

    void notify_prepared(request_context& rctx) override;
    void notify_completed(request_context& rctx) override;

    [[nodiscard]] model::step_kind kind() const noexcept override;

    /**
     * @brief declare the number of partitions
     * @details process step has the model level knowledge about the number of partitions this process can run.
     * E.g. the process contains some logic that forces to run it only on single partition.
     * This method is to calculate the information based on the graph information and to externalize the knowledge.
     * Subclass should override the default implementation to handle specific cases limiting the number of partitions.
     * @return the number of partitions
     */
    [[nodiscard]] virtual std::size_t partitions() const noexcept;

    // for testing
    void partitions(std::size_t num) noexcept;

    void activate(request_context& rctx) override;

    void executor_factory(std::shared_ptr<abstract::process_executor_factory> factory) noexcept;

    [[nodiscard]] std::shared_ptr<abstract::process_executor_factory> const& executor_factory() const noexcept;

    void io_info(std::shared_ptr<class io_info> arg) noexcept;

    [[nodiscard]] std::shared_ptr<class io_info> const& io_info() const noexcept;

    void relation_io_map(std::shared_ptr<class relation_io_map> arg) noexcept;

    [[nodiscard]] std::shared_ptr<class relation_io_map> const& relation_io_map() const noexcept;

    void io_exchange_map(std::shared_ptr<class io_exchange_map> arg) noexcept;

    [[nodiscard]] std::shared_ptr<class io_exchange_map> const& io_exchange_map() const noexcept;

private:
    std::shared_ptr<processor_info> info_{};
    std::shared_ptr<abstract::process_executor_factory> executor_factory_{};
    std::shared_ptr<class io_info> io_info_{};
    std::size_t partitions_{global::config_pool()->default_partitions()};
    std::shared_ptr<class relation_io_map> relation_io_map_{};
    std::shared_ptr<class io_exchange_map> io_exchange_map_{std::make_shared<class io_exchange_map>()};

    std::shared_ptr<class io_info> create_io_info();
};

} // namespace jogasaki::executor::process
