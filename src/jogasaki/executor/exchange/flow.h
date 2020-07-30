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

#include <takatori/util/reference_list_view.h>
#include <takatori/util/universal_extractor.h>

#include <jogasaki/model/port.h>
#include <jogasaki/model/step.h>
#include <jogasaki/executor/common/flow.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/task.h>
#include <jogasaki/executor/exchange/sink.h>
#include <jogasaki/executor/exchange/source.h>

namespace jogasaki::executor::exchange {

/**
 * @brief exchange step data flow
 */
class flow : public common::flow {
public:
    using sink_list_view = takatori::util::reference_list_view<takatori::util::universal_extractor<exchange::sink>>;

    using source_list_view = takatori::util::reference_list_view<takatori::util::universal_extractor<exchange::source>>;

    using sinks_sources = std::pair<sink_list_view, source_list_view>;

    /**
     * @brief a function to tell the exchange data flow object about the number of partitions required
     * @param partitions the number of partitions requested
     * @return list view of sinks and sources newly created by this call
     */
    [[nodiscard]] virtual sinks_sources setup_partitions(std::size_t partitions) = 0;

    /**
     * @brief accessor for sinks
     * @return list view of sinks held by this exchange
     */
    [[nodiscard]] virtual sink_list_view sinks() = 0;

    /**
     * @brief accessor for sources
     * @return list view of sources held by this exchange
     */
    [[nodiscard]] virtual source_list_view sources() = 0;

    [[nodiscard]] takatori::util::sequence_view<std::shared_ptr<model::task>> create_pretask(port_index_type) override {
        // exchanges don't have sub input ports
        return {};
    }
};

} // namespace

