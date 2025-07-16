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

#include <vector>

#include <jogasaki/model/port.h>
#include <jogasaki/model/step.h>
#include <jogasaki/model/flow.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/task.h>
#include <jogasaki/executor/exchange/sink.h>
#include <jogasaki/executor/exchange/source.h>

namespace jogasaki::executor::exchange {

/**
 * @brief exchange step data flow
 */
class flow : public model::flow {
public:
    /**
     * @brief a function to tell the exchange data flow object about the number of partitions required
     * @param partitions the number of partitions requested
     * @return list view of sinks and sources newly created by this call
     */
    virtual void setup_partitions(std::size_t partitions) = 0;

    /**
     * @brief accessor for sink count
     * @return number of sinks held by this exchange
     */
    [[nodiscard]] virtual std::size_t sink_count() const = 0;

    /**
     * @brief accessor for source count
     * @return number of sources held by this exchange
     */
    [[nodiscard]] virtual std::size_t source_count() const = 0;

    /**
     * @brief accessor for sink
     * @param index index of sink
     * @return sink object at index
     */
    [[nodiscard]] virtual exchange::sink& sink_at(std::size_t index) = 0;

    /**
     * @brief accessor for source
     * @param index index of source
     * @return source object at index
     */
    [[nodiscard]] virtual exchange::source& source_at(std::size_t index) = 0;

    [[nodiscard]] takatori::util::sequence_view<std::shared_ptr<model::task>> create_pretask(port_index_type) override {
        // exchanges don't have sub input ports
        return {};
    }
};

}  // namespace jogasaki::executor::exchange
