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
#include "cogroup_task.h"

namespace jogasaki::executor {

template<class T>
using sequence_view = takatori::util::sequence_view<T>;

class cogroup_flow : public common::flow {
public:
    cogroup_flow() = default;
    cogroup_flow(
            exchange::step* upstream,
            model::step* step,
            channel* ch,
            std::shared_ptr<meta::group_meta> meta
    ) :
            upstream_(upstream),
            step_(step),
            channel_(ch),
            meta_(std::move(meta))
    {}

    sequence_view<std::unique_ptr<model::task>> create_tasks() override {
        auto srcs = dynamic_cast<exchange::group::flow&>(upstream_->data_flow_object()).sources();
        for(auto& s : srcs) {
            tasks_.emplace_back(std::make_unique<cogroup_task>(channel_, step_, s.acquire_reader(), meta_));
        }
        return takatori::util::sequence_view{&*(tasks_.begin()), &*(tasks_.end())};
    }

    sequence_view<std::unique_ptr<model::task>> create_pretask(port_index_type) override {
        return {};
    }

    [[nodiscard]] common::step_kind kind() const noexcept override {
        return common::step_kind::process;
    }
private:
    std::vector<std::unique_ptr<model::task>> tasks_{};
    exchange::step* upstream_{};
    model::step* step_{};
    channel* channel_{};
    std::shared_ptr<meta::group_meta> meta_{};
};

}
