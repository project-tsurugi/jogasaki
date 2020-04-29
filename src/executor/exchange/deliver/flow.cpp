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
#include <vector>

#include "flow.h"
#include <executor/exchange/group/writer.h>
#include "sink.h"
#include "source.h"

namespace jogasaki::executor::exchange::deliver {

namespace impl {

flow::source_list_view cast_to_exchange_source(std::vector<std::unique_ptr<deliver::source>>& vp) {
    takatori::util::universal_extractor<exchange::source> ext {
            [](void* cursor) -> exchange::source& {
                return static_cast<exchange::source&>(**static_cast<std::unique_ptr<deliver::source>*>(cursor));
            },
            [](void* cursor, std::ptrdiff_t offset) {
                return static_cast<void*>(static_cast<std::unique_ptr<deliver::source>*>(cursor) + offset);
            },
    };
    return flow::source_list_view{ vp, ext };
}
flow::sink_list_view cast_to_exchange_sink(std::vector<std::unique_ptr<deliver::sink>>& vp) {
    takatori::util::universal_extractor<exchange::sink> ext {
            [](void* cursor) -> exchange::sink& {
                return static_cast<exchange::sink&>(**static_cast<std::unique_ptr<deliver::sink>*>(cursor));
            },
            [](void* cursor, std::ptrdiff_t offset) {
                return static_cast<void*>(static_cast<std::unique_ptr<deliver::sink>*>(cursor) + offset);
            },
    };
    return flow::sink_list_view{ vp, ext };
}

} // namespace impl

flow::~flow() = default;

flow::flow(channel* ch, model::step* step) : channel_(ch), owner_(step) {}

takatori::util::sequence_view<std::unique_ptr<model::task>> flow::create_tasks() {
    tasks_.emplace_back(std::make_unique<exchange::task>(channel_, dynamic_cast<exchange::step*>(owner_)));
    return tasks_;
}

flow::sinks_sources flow::setup_partitions(std::size_t ) {
    return std::pair(impl::cast_to_exchange_sink(sinks_), impl::cast_to_exchange_source(sources_));
}

flow::source_list_view flow::sources() {
    return impl::cast_to_exchange_source(sources_);
}

} // namespace


