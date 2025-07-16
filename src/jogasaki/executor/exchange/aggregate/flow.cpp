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

#include <jogasaki/executor/exchange/aggregate/input_partition.h>
#include <jogasaki/executor/exchange/aggregate/sink.h>
#include <jogasaki/executor/exchange/aggregate/source.h>
#include <jogasaki/executor/exchange/aggregate/writer.h>
#include <jogasaki/executor/exchange/shuffle/run_info.h>
#include <jogasaki/executor/exchange/sink.h>
#include <jogasaki/executor/exchange/source.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/task.h>
#include <jogasaki/status.h>

#include <ext/alloc_traits.h>

namespace jogasaki::executor::exchange::aggregate {

flow::~flow() = default;
flow::flow() : info_(std::make_shared<aggregate_info>()) {}
flow::flow(
    std::shared_ptr<aggregate_info> info,
    request_context* context,
    step* owner, std::size_t downstream_partitions
) :
    info_(std::move(info)),
    context_(context),
    owner_(owner),
    downstream_partitions_(downstream_partitions),
    generate_record_on_empty_(info_->generate_record_on_empty())
{}

takatori::util::sequence_view<std::shared_ptr<model::task>> flow::create_tasks() {
    tasks_.emplace_back(std::make_shared<exchange::task>(context_, owner_));
    transfer();
    return tasks_;
}

void flow::setup_partitions(std::size_t partitions) {
    // assuming aggregate exchange has only one output, so this is called only once
    sinks_.reserve(partitions);
    for(std::size_t i=0; i < partitions; ++i) {
        sinks_.emplace_back(std::make_unique<aggregate::sink>(downstream_partitions_, info_, context()));
    }
    sources_.reserve(downstream_partitions_);
    for(std::size_t i=0; i < downstream_partitions_; ++i) {
        sources_.emplace_back(std::make_unique<source>(info_, context()));
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
        for(auto& p : partitions) {
            if (! p) continue;
            empty = false;
        }
    }
    updatable_info().empty_input(empty);
    if (generate_record_on_empty_ && empty && context_->status_code() == status::ok) {
        // generate a special record for empty input
        // unless an error happens on upstream of this exchange (in that case adding the record for empty input
        // behaves like reading empty records successfully) Canceling processing the output record on the downstream
        // steps is not implemented yet.
        auto& partitions = sinks_[0]->input_partitions();
        auto& p = partitions.emplace_back(std::make_unique<input_partition>(info_));
        p->aggregate_empty_input();
    }
    for(auto& sink : sinks_) {
        auto& partitions = sink->input_partitions();
        BOOST_ASSERT(partitions.size() <= sources_.size()); //NOLINT
        for(std::size_t i=0, n=sources_.size(); i < n; ++i) {
            if (i >= partitions.size() || !partitions[i]) continue;
            partitions[i]->release_hashtable();
            sources_[i]->receive(std::move(partitions[i]));
        }
    }
    transfer_completed();
}

} // namespace


