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
#include <jogasaki/executor/exchange/group/writer.h>

#include <vector>

namespace jogasaki::executor::exchange::forward {

namespace impl {

flow::source_list_view cast_to_exchange_source(std::vector<std::unique_ptr<forward::source>>& vp) {
    takatori::util::universal_extractor<exchange::source> ext {
            [](void* cursor) -> exchange::source& {
                return static_cast<exchange::source&>(**static_cast<std::unique_ptr<forward::source>*>(cursor));
            },
            [](void* cursor, std::ptrdiff_t offset) {
                return static_cast<void*>(static_cast<std::unique_ptr<forward::source>*>(cursor) + offset); //NOLINT
            },
    };
    return flow::source_list_view{ vp, ext };
}

flow::sink_list_view cast_to_exchange_sink(std::vector<std::unique_ptr<forward::sink>>& vp) {
    takatori::util::universal_extractor<exchange::sink> ext {
            [](void* cursor) -> exchange::sink& {
                return static_cast<exchange::sink&>(**static_cast<std::unique_ptr<forward::sink>*>(cursor));
            },
            [](void* cursor, std::ptrdiff_t offset) {
                return static_cast<void*>(static_cast<std::unique_ptr<forward::sink>*>(cursor) + offset); //NOLINT
            },
    };
    return flow::sink_list_view{ vp, ext };
}

} // namespace impl

flow::flow(
    std::shared_ptr<meta::record_meta> input_meta,
    request_context* context,
    step* owner
) :
    input_meta_(std::move(input_meta)),
    context_(context),
    owner_(owner)
{
    (void)context_;

    // For testing purpose, setup sinks/sources automatically even if there is no input.
    if (owner_->input_ports().empty()) {
        setup_partitions(default_partitions);
    }
}

takatori::util::sequence_view<std::shared_ptr<model::task>> flow::create_tasks() {
    // no tasks for forward
    return tasks_;
}

flow::sinks_sources flow::setup_partitions(std::size_t partitions) {
    sinks_.reserve(sinks_.size() + partitions);
    for(std::size_t i=0; i < partitions; ++i) {
        sinks_.emplace_back(std::make_unique<forward::sink>());
    }

    sources_.reserve(sources_.size() + partitions);
    for(std::size_t i=0; i < partitions; ++i) {
        sources_.emplace_back(std::make_unique<forward::source>());
    }

    return std::pair(impl::cast_to_exchange_sink(sinks_),
            impl::cast_to_exchange_source(sources_));
}

flow::sink_list_view flow::sinks() {
    return impl::cast_to_exchange_sink(sinks_);
}

flow::source_list_view flow::sources() {
    return impl::cast_to_exchange_source(sources_);
}


} // namespace


