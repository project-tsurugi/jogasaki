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
#include <vector>

#include <takatori/util/sequence_view.h>

#include <jogasaki/constants.h>
#include <jogasaki/executor/exchange/aggregate/aggregate_info.h>
#include <jogasaki/executor/exchange/flow.h>
#include <jogasaki/executor/exchange/shuffle/flow.h>
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

namespace jogasaki::executor::exchange::aggregate {

/**
 * @brief group step data flow
 */
class flow : public shuffle::flow {
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
    flow();

    /**
     * @brief create new instance
     * @param input_meta input record metadata
     * @param key_indices indices for key fields
     */
    flow(std::shared_ptr<aggregate_info> info,
            request_context* context,
            step* owner,
            std::size_t downstream_partitions);

    [[nodiscard]] takatori::util::sequence_view<std::shared_ptr<model::task>> create_tasks() override;

    void setup_partitions(std::size_t partitions) override;

    [[nodiscard]] std::size_t sink_count() const noexcept override;

    [[nodiscard]] std::size_t source_count() const noexcept override;

    [[nodiscard]] exchange::sink& sink_at(std::size_t index) override;

    [[nodiscard]] exchange::source& source_at(std::size_t index) override;

    /**
     * @brief transfer the input partitions from sinks to sources
     */
    void transfer();

    /**
     * @copydoc
     */
    [[nodiscard]] model::step_kind kind() const noexcept override {
        return model::step_kind::aggregate;
    }

    [[nodiscard]] class request_context* context() const noexcept {
        return context_;
    }
private:
    std::vector<std::shared_ptr<model::task>> tasks_{};
    std::shared_ptr<aggregate_info> info_{};
    std::vector<std::unique_ptr<aggregate::sink>> sinks_;
    std::vector<std::unique_ptr<aggregate::source>> sources_{};
    class request_context* context_{};
    step* owner_{};
    std::size_t downstream_partitions_{global::config_pool()->default_partitions()};
    bool generate_record_on_empty_{false};
};

} // namespace jogasaki::executor::exchange::aggregate


