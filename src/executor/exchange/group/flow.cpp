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
#include "flow.h"
#include <executor/exchange/group/writer.h>

#include <vector>

namespace jogasaki::executor::exchange::group {

namespace impl {

flow::source_list_view cast_to_exchange_source(std::vector<std::unique_ptr<group::source>>& vp) {
    takatori::util::universal_extractor<exchange::source> ext {
            [](void* cursor) -> exchange::source& {
                return static_cast<exchange::source&>(**static_cast<std::unique_ptr<group::source>*>(cursor));
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
                return static_cast<exchange::sink&>(**static_cast<std::unique_ptr<group::sink>*>(cursor));
            },
            [](void* cursor, std::ptrdiff_t offset) {
                return static_cast<void*>(static_cast<std::unique_ptr<group::sink>*>(cursor) + offset); //NOLINT
            },
    };
    return flow::sink_list_view{ vp, ext };
}

} // namespace impl

flow::~flow() = default;
flow::flow() : info_(std::make_shared<shuffle_info>()) {}
flow::flow(std::shared_ptr<shuffle_info> info, channel* ch, step* owner, std::size_t downstream_partitions) :
        info_(std::move(info)), channel_(ch), owner_(owner), downstream_partitions_(downstream_partitions) {}

flow::flow(std::shared_ptr<meta::record_meta> input_meta,
        std::vector<field_index_type> key_indices,
        channel* ch,
        step* owner,
        std::size_t downstream_partitions
) :
        flow(std::make_shared<shuffle_info>(std::move(input_meta), std::move(key_indices)), ch, owner, downstream_partitions) {}

takatori::util::sequence_view<std::unique_ptr<model::task>> flow::create_tasks() {
    tasks_.emplace_back(std::make_unique<exchange::task>(channel_, owner_));
    transfer();
    return tasks_;
}

flow::sinks_sources flow::setup_partitions(std::size_t partitions) {
    sinks_.reserve(sinks_.size() + partitions);
    for(std::size_t i=0; i < partitions; ++i) {
        sinks_.emplace_back(std::make_unique<group::sink>(downstream_partitions_, info_));
    }

    sources_.reserve(downstream_partitions_);
    for(std::size_t i=0; i < downstream_partitions_; ++i) {
        sources_.emplace_back(std::make_unique<source>(info_));
    }

    return std::pair(impl::cast_to_exchange_sink(sinks_),
            impl::cast_to_exchange_source(sources_));
}

flow::source_list_view flow::sources() {
    return impl::cast_to_exchange_source(sources_);
}

void flow::transfer() {
    for(auto& sink : sinks_) {
        auto& partitions = sink->input_partitions();
        assert(partitions.size() == sources_.size()); //NOLINT
        for(std::size_t i=0; i < partitions.size(); ++i) {
            sources_[i]->receive(std::move(partitions[i]));
        }
    }
}

} // namespace


