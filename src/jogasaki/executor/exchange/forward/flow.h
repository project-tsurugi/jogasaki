/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>

#include <jogasaki/constants.h>
#include <jogasaki/executor/exchange/flow.h>
#include <jogasaki/executor/exchange/forward/forward_info.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/task.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/model/port.h>
#include <jogasaki/model/step.h>
#include <jogasaki/model/step_kind.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>

#include "sink.h"
#include "source.h"

namespace jogasaki::executor::exchange::forward {

/**
 * @brief forward step data flow
 */
class flow : public exchange::flow {
public:
    using field_index_type = meta::record_meta::field_index_type;

    ~flow() override;

    flow(flow const& other) = default;
    flow& operator=(flow const& other) = default;
    flow(flow&& other) noexcept = default;
    flow& operator=(flow&& other) noexcept = default;

    /**
     * @brief create new instance with empty schema (for testing)
     */
    flow() = default;

    /**
     * @brief create new instance
     * @param info the forward exchange information
     * @param context the request context
     * @param owner the step that owns this flow
     */
    flow(
        std::shared_ptr<forward_info> info,
        request_context* context,
        step* owner
    );

    [[nodiscard]] takatori::util::sequence_view<std::shared_ptr<model::task>> create_tasks() override;

    void setup_partitions(std::size_t partitions) override;

    [[nodiscard]] std::size_t sink_count() const noexcept override;

    [[nodiscard]] std::size_t source_count() const noexcept override;

    [[nodiscard]] exchange::sink& sink_at(std::size_t index) override;

    [[nodiscard]] exchange::source& source_at(std::size_t index) override;

    [[nodiscard]] model::step_kind kind() const noexcept override;

    [[nodiscard]] class request_context* context() const noexcept;

private:
    std::vector<std::shared_ptr<model::task>> tasks_{};
    std::shared_ptr<forward_info> info_{};
    std::deque<std::unique_ptr<forward::sink>> sinks_{};  // use deque to avoid relocation
    std::deque<std::unique_ptr<forward::source>> sources_{}; // use deque to avoid relocation
    request_context* context_{};
    step* owner_{};
};

}  // namespace jogasaki::executor::exchange::forward
