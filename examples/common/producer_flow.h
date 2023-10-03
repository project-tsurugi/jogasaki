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
#pragma once

#include <memory>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/process/step.h>
#include "producer_task.h"

namespace jogasaki::common_cli {

using takatori::util::maybe_shared_ptr;

template<class T>
using sequence_view = takatori::util::sequence_view<T>;

template <class Params>
class producer_flow : public model::flow {
public:
    producer_flow() = default;
    producer_flow(executor::exchange::step* downstream,
            model::step* step,
            request_context* context,
            maybe_shared_ptr<meta::record_meta> meta,
            Params& p) :
            downstream_(downstream),
            step_(step),
            context_(context),
            meta_(std::move(meta)),
            params_(&p) {}

    sequence_view<std::shared_ptr<model::task>> create_tasks() override {
        auto [sinks, srcs] = dynamic_cast<executor::exchange::flow&>(downstream_->data_flow_object(*context_)).setup_partitions(params_->upstream_partitions_);
        (void)srcs;
        resources_.reserve(sinks.size());
        tasks_.reserve(sinks.size());
        for(auto& s : sinks) {
            auto& resource = resources_.emplace_back(std::make_unique<memory::monotonic_paged_memory_resource>(&global::page_pool()));
            tasks_.emplace_back(std::make_unique<producer_task<Params>>(context_, step_, &s, meta_, *params_, *resource));
        }
        return takatori::util::sequence_view{&*(tasks_.begin()), &*(tasks_.end())};
    }

    sequence_view<std::shared_ptr<model::task>> create_pretask(port_index_type) override {
        return {};
    }

    [[nodiscard]] model::step_kind kind() const noexcept override {
        return model::step_kind::process;
    }

private:
    std::vector<std::shared_ptr<model::task>> tasks_{};
    executor::exchange::step* downstream_{};
    model::step* step_{};
    request_context* context_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
    Params* params_{};
    std::vector<std::unique_ptr<memory::monotonic_paged_memory_resource>> resources_{};
};

}
