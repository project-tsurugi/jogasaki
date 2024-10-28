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
#include "flow.h"

#include <algorithm>
#include <utility>
#include <vector>
#include <glog/logging.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/exchange/flow.h>
#include <jogasaki/executor/exchange/forward/reader.h>
#include <jogasaki/executor/exchange/forward/sink.h>
#include <jogasaki/executor/exchange/forward/source.h>
#include <jogasaki/executor/exchange/forward/writer.h>
#include <jogasaki/executor/exchange/sink.h>
#include <jogasaki/executor/exchange/source.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor::exchange::forward {

flow::~flow() = default;

flow::flow(
    std::shared_ptr<forward_info> info,
    request_context* context,
    step* owner
) :
    info_(std::move(info)),
    context_(context),
    owner_(owner)
{}

takatori::util::sequence_view<std::shared_ptr<model::task>> flow::create_tasks() {
    tasks_.emplace_back(std::make_shared<exchange::task>(context_, owner_));
    return tasks_;
}

void flow::setup_partitions(std::size_t partitions) {
    // additional sinks/sources for requested partitions
    std::vector<std::shared_ptr<input_partition>> shared_partitions{};
    std::shared_ptr<std::atomic_size_t> write_count{
        info_->limit().has_value() ? std::make_shared<std::atomic_size_t>(0) : nullptr
    };
    shared_partitions.reserve(partitions);
    for(std::size_t i=0; i < partitions; ++i) {
        shared_partitions.emplace_back(std::make_shared<input_partition>(info_));
    }
    for(std::size_t i=0; i < partitions; ++i) {
        sinks_.emplace_back(
            std::make_unique<forward::sink>(info_, context_, write_count, shared_partitions[i])
        );
    }
    for(std::size_t i=0; i < partitions; ++i) {
        sources_.emplace_back(std::make_unique<forward::source>(
            info_,
            context_,
            std::move(shared_partitions[i])
        ));
    }
    VLOG_LP(log_trace) << "added new srcs/sinks flow:" << this << ") partitions:" << partitions
                       << " #sinks:" << sinks_.size() << " #sources:" << sources_.size();
}

std::size_t flow::sink_count() const noexcept {
    return sinks_.size();
}

std::size_t flow::source_count() const noexcept {
    return sources_.size();
}

exchange::sink& flow::sink_at(std::size_t index) {
    return *sinks_[index];
}

exchange::source& flow::source_at(std::size_t index) {
    return *sources_[index];
}

class request_context* flow::context() const noexcept {
    return context_;
}

model::step_kind flow::kind() const noexcept {
    return model::step_kind::forward;
}

}  // namespace jogasaki::executor::exchange::forward
