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
#pragma once

#include <memory>

#include <model/step.h>
#include <model/task.h>
#include <constants.h>
#include <executor/process/step.h>
#include <zconf.h>
#include "consumer_task.h"
#include "params.h"

namespace jogasaki::group_cli {

template<class T>
using sequence_view = takatori::util::sequence_view<T>;

class consumer_flow : public executor::common::flow {
public:
    consumer_flow() = default;
    consumer_flow(
            executor::exchange::step* upstream,
            model::step* step,
            std::shared_ptr<request_context> context,
            std::shared_ptr<meta::group_meta> meta,
            params& c
    ) :
            upstream_(upstream),
            step_(step),
            context_(std::move(context)),
            meta_(std::move(meta)),
            params_(&c)
    {}

    sequence_view<std::shared_ptr<model::task>> create_tasks() override {
        auto srcs = dynamic_cast<executor::exchange::group::flow&>(upstream_->data_flow_object()).sources();
        tasks_.reserve(srcs.size());
        for(auto& s : srcs) {
            tasks_.emplace_back(std::make_unique<consumer_task>(context_, step_, s.acquire_reader(), meta_, *params_));
        }
        return takatori::util::sequence_view{&*(tasks_.begin()), &*(tasks_.end())};
    }

    sequence_view<std::shared_ptr<model::task>> create_pretask(port_index_type) override {
        return {};
    }

    [[nodiscard]] executor::common::step_kind kind() const noexcept override {
        return executor::common::step_kind::process;
    }

private:
    std::vector<std::shared_ptr<model::task>> tasks_{};
    executor::exchange::step* upstream_{};
    model::step* step_{};
    std::shared_ptr<request_context> context_{};
    std::shared_ptr<meta::group_meta> meta_{};
    params* params_{};
};

}
