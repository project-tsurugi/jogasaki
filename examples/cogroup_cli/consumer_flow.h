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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/process/step.h>
#include "consumer_task.h"
#include "priority_queue_consumer_task.h"
#include "params.h"

namespace jogasaki::cogroup_cli {

using takatori::util::maybe_shared_ptr;

template<class T>
using sequence_view = takatori::util::sequence_view<T>;

class consumer_flow : public executor::common::flow {
public:
    consumer_flow() = default;
    consumer_flow(
            executor::exchange::step* left_upstream,
            executor::exchange::step* right_upstream,
            model::step* step,
            request_context* context,
            maybe_shared_ptr<meta::group_meta> meta,
            params& c
    ) :
            left_upstream_(left_upstream),
            right_upstream_(right_upstream),
            step_(step),
            context_(context),
            meta_(std::move(meta)),
            params_(&c)
    {}

    sequence_view<std::shared_ptr<model::task>> create_tasks() override {
        auto l_srcs = dynamic_cast<executor::exchange::group::flow&>(left_upstream_->data_flow_object()).sources();
        auto r_srcs = dynamic_cast<executor::exchange::group::flow&>(right_upstream_->data_flow_object()).sources();
        tasks_.reserve(l_srcs.size());
        assert(l_srcs.size() == r_srcs.size());
        for(std::size_t i = 0, n = l_srcs.size(); i < n; ++i) {
            if (params_->use_priority_queue) {
                tasks_.emplace_back(std::make_unique<priority_queue_consumer_task>(context_, step_, l_srcs[i].acquire_reader(), r_srcs[i].acquire_reader(), meta_, meta_,*params_));
            } else {
                tasks_.emplace_back(std::make_unique<consumer_task>(context_, step_, l_srcs[i].acquire_reader(), r_srcs[i].acquire_reader(), meta_, meta_,*params_));
            }
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
    executor::exchange::step* left_upstream_{};
    executor::exchange::step* right_upstream_{};
    model::step* step_{};
    request_context* context_{};
    maybe_shared_ptr<meta::group_meta> meta_{};
    params* params_{};
};

}
