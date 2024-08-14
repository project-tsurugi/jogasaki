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

#include <takatori/util/downcast.h>
#include <takatori/util/universal_extractor.h>

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

using takatori::util::unsafe_downcast;

namespace impl {

flow::source_list_view cast_to_exchange_source(std::vector<std::unique_ptr<forward::source>>& vp) {
    takatori::util::universal_extractor<exchange::source> ext {
            [](void* cursor) -> exchange::source& {
                return unsafe_downcast<exchange::source>(**static_cast<std::unique_ptr<forward::source>*>(cursor));
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
                return unsafe_downcast<exchange::sink>(**static_cast<std::unique_ptr<forward::sink>*>(cursor));
            },
            [](void* cursor, std::ptrdiff_t offset) {
                return static_cast<void*>(static_cast<std::unique_ptr<forward::sink>*>(cursor) + offset); //NOLINT
            },
    };
    return flow::sink_list_view{ vp, ext };
}

} // namespace impl

flow::~flow() = default;

// flow::flow() : info_(std::make_shared<forward_info>()) {}
flow::flow(
    std::shared_ptr<forward_info> info,
    request_context* context,
    step* owner, std::size_t downstream_partitions
) :
    info_(std::move(info)),
    context_(context),
    owner_(owner),
    downstream_partitions_(downstream_partitions)
{}

/*
flow::flow(
    maybe_shared_ptr<meta::record_meta> input_meta,
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
        (void)setup_partitions(default_partitions);
    }
}
*/

takatori::util::sequence_view<std::shared_ptr<model::task>> flow::create_tasks() {
    tasks_.emplace_back(std::make_shared<exchange::task>(context_, owner_));
    return tasks_;
}

flow::sinks_sources flow::setup_partitions(std::size_t partitions) {
    // additional sinks/sources for requested partitions
    sinks_.reserve(sinks_.size() + partitions);
    std::vector<std::shared_ptr<std::atomic_bool>> active_flags{};
    active_flags.reserve(partitions);
    for(std::size_t i=0; i < partitions; ++i) {
        active_flags.emplace_back(std::make_shared<std::atomic_bool>(true));
    }
    for(std::size_t i=0; i < partitions; ++i) {
        sinks_.emplace_back(std::make_unique<forward::sink>(0, info_, context_, active_flags[i]));
    }
    sources_.reserve(sources_.size() + partitions);
    for(std::size_t i=0; i < partitions; ++i) {
        sources_.emplace_back(std::make_unique<forward::source>(info_, context_, sinks_[i]->partition(), active_flags[i]));
    }

    VLOG_LP(log_trace) << "forward exchange partitions:" << partitions << " sinks:" << sinks_.size() << " sources:" << sources_.size();

    return {impl::cast_to_exchange_sink(sinks_),
            impl::cast_to_exchange_source(sources_)};
}

flow::sink_list_view flow::sinks() {
    return impl::cast_to_exchange_sink(sinks_);
}

flow::source_list_view flow::sources() {
    return impl::cast_to_exchange_source(sources_);
}

class request_context* flow::context() const noexcept {
    return context_;
}

model::step_kind flow::kind() const noexcept {
    return model::step_kind::forward;
}

}  // namespace jogasaki::executor::exchange::forward
