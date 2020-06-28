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

#include <vector>

#include <jogasaki/model/port.h>
#include <jogasaki/model/step.h>
#include <jogasaki/executor/exchange/flow.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/task.h>
#include <jogasaki/executor/common/step_kind.h>
#include "sink.h"
#include "source.h"

namespace jogasaki::executor::exchange::group {

namespace impl {

flow::source_list_view cast_to_exchange_source(std::vector<std::unique_ptr<group::source>>& vp);
flow::sink_list_view cast_to_exchange_sink(std::vector<std::unique_ptr<group::sink>>& vp);

} // namespace impl

/**
 * @brief group step data flow
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
    flow();

    /**
     * @brief create new instance
     * @param input_meta input record metadata
     * @param key_indices indices for key fields
     */
    flow(std::shared_ptr<shuffle_info> info,
            std::shared_ptr<request_context> context,
            step* owner,
            std::size_t downstream_partitions);

    /**
     * @brief create new instance
     * @param input_meta input record metadata
     * @param key_indices indices for key fields
     */
    flow(std::shared_ptr<meta::record_meta> input_meta,
            std::vector<field_index_type> key_indices,
            std::shared_ptr<request_context> context,
            step* owner,
            std::size_t downstream_partitions
            );

    takatori::util::sequence_view<std::shared_ptr<model::task>> create_tasks() override;

    sinks_sources setup_partitions(std::size_t partitions) override;

    sink_list_view sinks() override;

    source_list_view sources() override;

    /**
     * @brief transfer the input partitions from sinks to sources
     */
    void transfer();

    /**
     * @copydoc
     */
    [[nodiscard]] common::step_kind kind() const noexcept override {
        return common::step_kind::group;
    }

    [[nodiscard]] std::shared_ptr<class request_context> const& context() const noexcept {
        return context_;
    }
private:
    std::vector<std::shared_ptr<model::task>> tasks_{};
    std::shared_ptr<shuffle_info> info_{};
    std::vector<std::unique_ptr<group::sink>> sinks_;
    std::vector<std::unique_ptr<group::source>> sources_{};
    std::shared_ptr<class request_context> context_{};
    step* owner_{};
    std::size_t downstream_partitions_{default_partitions};
};

}


