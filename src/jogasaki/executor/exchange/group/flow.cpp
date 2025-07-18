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
#include "flow.h"

#include <utility>
#include <vector>
#include <boost/assert.hpp>

#include <jogasaki/executor/exchange/group/input_partition.h>
#include <jogasaki/executor/exchange/group/sink.h>
#include <jogasaki/executor/exchange/group/source.h>
#include <jogasaki/executor/exchange/group/writer.h>
#include <jogasaki/executor/exchange/shuffle/run_info.h>
#include <jogasaki/executor/exchange/sink.h>
#include <jogasaki/executor/exchange/source.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/task.h>
#include <jogasaki/model/step_kind.h>

namespace jogasaki::executor::exchange::group {

flow::~flow() = default;
flow::flow() : info_(std::make_shared<group_info>()) {}
flow::flow(
    std::shared_ptr<group_info> info,
    request_context* context,
    step* owner, std::size_t downstream_partitions
) :
    info_(std::move(info)),
    context_(context),
    owner_(owner),
    downstream_partitions_(downstream_partitions)
{}

flow::flow(
    maybe_shared_ptr<meta::record_meta> input_meta,
    std::vector<field_index_type> key_indices,
    request_context* context,
    step* owner,
    std::size_t downstream_partitions
) :
    flow(
        std::make_shared<group_info>(std::move(input_meta), std::move(key_indices)),
        context,
        owner,
        downstream_partitions
    )
{}

takatori::util::sequence_view<std::shared_ptr<model::task>> flow::create_tasks() {
    tasks_.emplace_back(std::make_shared<exchange::task>(context_, owner_));
    transfer();
    return tasks_;
}

void flow::setup_partitions(std::size_t partitions) {
    // note this function can be called multiple times (e.g. multiple input process)
    // add sinks for the requested partitions, while keep the number of sources same as `downstream_partitions_`
    for(std::size_t i=0; i < partitions; ++i) {
        sinks_.emplace_back(std::make_unique<group::sink>(downstream_partitions_, info_, context()));
    }
    if(sources_.size() < downstream_partitions_) {
        sources_.reserve(downstream_partitions_);
        for(std::size_t i=0; i < downstream_partitions_; ++i) {
            sources_.emplace_back(std::make_unique<source>(info_, context()));
        }
    }
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

void flow::transfer() {
    bool empty = true;
    for(auto& sink : sinks_) {
        auto& partitions = sink->input_partitions();
        BOOST_ASSERT(partitions.size() <= sources_.size()); //NOLINT
        for(std::size_t i=0, n=sources_.size(); i < n; ++i) {
            if (i >= partitions.size() || ! partitions[i]) continue;
            sources_[i]->receive(std::move(partitions[i]));
            empty = false;
        }
    }
    updatable_info().empty_input(empty);
    transfer_completed();
}

class request_context* flow::context() const noexcept {
    return context_;
}

model::step_kind flow::kind() const noexcept {
    return model::step_kind::group;
}

}  // namespace jogasaki::executor::exchange::group
