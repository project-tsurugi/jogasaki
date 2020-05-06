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

#include <model/port.h>
#include <model/step.h>
#include <executor/exchange/flow.h>
#include <executor/exchange/step.h>
#include <executor/exchange/task.h>
#include <executor/common/step_kind.h>
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

    ~flow();
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
    flow(std::shared_ptr<shuffle_info> info, channel* channel, step* owner);

    /**
     * @brief create new instance
     * @param input_meta input record metadata
     * @param key_indices indices for key fields
     */
    flow(std::shared_ptr<meta::record_meta> input_meta,
            std::vector<field_index_type> key_indices,
            channel* ch,
            step* owner
            );

    takatori::util::sequence_view<std::unique_ptr<model::task>> create_tasks() override;

    sinks_sources setup_partitions(std::size_t partitions) override;

    source_list_view sources() override;

    void transfer();

    [[nodiscard]] common::step_kind kind() const noexcept override {
        return common::step_kind::group;
    }

    /**
     * @brief request downstream partitions
     * @details downstream process can use this to instruct the number of partitions
     * @param arg the number of downstream partitions
     * @attention To configure the downstream partitions, this should be called before setup_partitions.
     */
    void downstream_partitions(std::size_t arg) {
        downstream_partitions_ = arg;
    }

private:
    std::vector<std::unique_ptr<model::task>> tasks_{};
    std::shared_ptr<shuffle_info> info_{};
    std::vector<std::unique_ptr<group::sink>> sinks_;
    std::vector<std::unique_ptr<group::source>> sources_{};
    std::size_t downstream_partitions_{default_partitions};
    channel* channel_{};
    step* owner_{};
};

}


