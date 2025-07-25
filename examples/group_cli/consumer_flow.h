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

#include <memory>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/process/step.h>
#include "consumer_task.h"
#include "params.h"

namespace jogasaki::group_cli {

using takatori::util::maybe_shared_ptr;

template<class T>
using sequence_view = takatori::util::sequence_view<T>;

class consumer_flow : public model::flow {
public:
    consumer_flow() = default;
    consumer_flow(
            executor::exchange::step* upstream,
            model::step* step,
            request_context* context,
            maybe_shared_ptr<meta::group_meta> meta,
            params& c
    ) :
            upstream_(upstream),
            step_(step),
            context_(context),
            meta_(std::move(meta)),
            params_(&c)
    {}

    sequence_view<std::shared_ptr<model::task>> create_tasks() override {
        auto& flow = dynamic_cast<executor::exchange::group::flow&>(upstream_->data_flow_object(*context_));
        tasks_.reserve(flow.source_count());
        for(std::size_t i=0, n=flow.source_count(); i<n; ++i) {
            auto& s = flow.source_at(i);
            tasks_.emplace_back(std::make_unique<consumer_task>(context_, step_, s.acquire_reader(), meta_, *params_));
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
    executor::exchange::step* upstream_{};
    model::step* step_{};
    request_context* context_{};
    maybe_shared_ptr<meta::group_meta> meta_{};
    params* params_{};
};

}
