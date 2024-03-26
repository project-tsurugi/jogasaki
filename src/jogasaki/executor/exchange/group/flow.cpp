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

#include <utility>
#include <vector>
#include <boost/assert.hpp>

#include <takatori/util/downcast.h>
#include <takatori/util/universal_extractor.h>

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

using takatori::util::unsafe_downcast;

namespace impl {

flow::source_list_view cast_to_exchange_source(std::vector<std::unique_ptr<group::source>>& vp) {
    takatori::util::universal_extractor<exchange::source> ext {
            [](void* cursor) -> exchange::source& {
                return unsafe_downcast<exchange::source>(**static_cast<std::unique_ptr<group::source>*>(cursor));
            },
            [](void* cursor, std::ptrdiff_t offset) {
                return static_cast<void*>(static_cast<std::unique_ptr<group::source>*>(cursor) + offset); //NOLINT
            },
    };
    return flow::source_list_view{ vp, ext };
}

flow::sink_list_view cast_to_exchange_sink(std::vector<std::unique_ptr<group::sink>>& vp) {
    takatori::util::universal_extractor<exchange::sink> ext {
            [](void* cursor) -> exchange::sink& {
                return unsafe_downcast<exchange::sink>(**static_cast<std::unique_ptr<group::sink>*>(cursor));
            },
            [](void* cursor, std::ptrdiff_t offset) {
                return static_cast<void*>(static_cast<std::unique_ptr<group::sink>*>(cursor) + offset); //NOLINT
            },
    };
    return flow::sink_list_view{ vp, ext };
}

} // namespace impl

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

flow::sinks_sources flow::setup_partitions(std::size_t partitions) {
    sinks_.reserve(partitions);
    for(std::size_t i=0; i < partitions; ++i) {
        sinks_.emplace_back(std::make_unique<group::sink>(downstream_partitions_, info_, context()));
    }
    sources_.reserve(downstream_partitions_);
    for(std::size_t i=0; i < downstream_partitions_; ++i) {
        sources_.emplace_back(std::make_unique<source>(info_, context()));
    }

    return {impl::cast_to_exchange_sink(sinks_),
            impl::cast_to_exchange_source(sources_)};
}

flow::sink_list_view flow::sinks() {
    return impl::cast_to_exchange_sink(sinks_);
}

flow::source_list_view flow::sources() {
    return impl::cast_to_exchange_source(sources_);
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

} // namespace


