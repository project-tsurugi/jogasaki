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
#include "step.h"

#include <memory>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/record_meta.h>
#include "flow.h"

namespace jogasaki::executor::process {

using jogasaki::executor::process::impl::ops::io_info;
using takatori::util::unsafe_downcast;

std::shared_ptr<io_info> step::create_io_info() {
    auto io = std::make_shared<class io_info>();

    std::vector<impl::ops::input_info> inputs{};
    for(auto& in : input_ports()) {
        auto& xchg = *unsafe_downcast<exchange::step>(in->opposites()[0]->owner());
        switch(xchg.kind()) {
            case common::step_kind::forward: {
                auto& fwd = unsafe_downcast<exchange::forward::step>(xchg);
                inputs.emplace_back(
                    fwd.output_meta(),
                    fwd.output_order()
                );
                break;
            }
            case common::step_kind::group:
            case common::step_kind::aggregate:
            {
                auto& grp = unsafe_downcast<exchange::shuffle::step>(xchg);
                inputs.emplace_back(
                    grp.output_meta(),
                    grp.output_order()
                );
                break;
            }
            default:
                fail();
        }
    }
    std::vector<impl::ops::output_info> outputs{};
    for(auto& out : output_ports()) {
        auto& xchg = *unsafe_downcast<exchange::step>(out->opposites()[0]->owner());
        switch(xchg.kind()) {
            case common::step_kind::forward:
            case common::step_kind::group:
            case common::step_kind::aggregate: {
                auto& x = unsafe_downcast<exchange::step>(xchg);
                outputs.emplace_back(
                    x.input_meta(),
                    x.input_order()
                );
                break;
            }
            default:
                fail();
        }
    }
    return std::make_shared<class io_info>(
        std::move(inputs),
        std::move(outputs),
        io_info::external_output_entity_type{}
    );
}

step::step(
    std::shared_ptr<processor_info> info,
    std::shared_ptr<class relation_io_map> relation_io_map,
    std::shared_ptr<class io_info> io_info
) :
    info_(std::move(info)),
    io_info_(std::move(io_info)),
    relation_io_map_(std::move(relation_io_map))
{}

common::step_kind step::kind() const noexcept {
    return common::step_kind::process;
}

std::size_t step::partitions() const noexcept {
    if (info_->details().has_scan_operator() ||
        info_->details().has_find_operator()) {
        return 1;
    }
    return owner()->context()->configuration()->default_partitions();
}

void step::activate() {
    if(! io_info_) {
        io_info_ = create_io_info();
    }
    data_flow_object(std::make_unique<flow>(
        context(),
        this,
        info_
    ));
}

void step::notify_prepared() {
    // check if main inputs are already available
    // raise providing to start main tasks running soon
}

void step::notify_completed() {
    // destroy process buffer
}

void step::partitions(std::size_t num) noexcept {
    partitions_ = num;
}

void step::executor_factory(std::shared_ptr<abstract::process_executor_factory> factory) noexcept {
    executor_factory_ = std::move(factory);
}

std::shared_ptr<abstract::process_executor_factory> const& step::executor_factory() const noexcept {
    return executor_factory_;
}

void step::io_info(std::shared_ptr<class io_info> arg) noexcept {
    io_info_ = std::move(arg);
}

std::shared_ptr<class io_info> const& step::io_info() const noexcept {
    return io_info_;
}

void step::relation_io_map(std::shared_ptr<class relation_io_map> arg) noexcept {
    relation_io_map_ = std::move(arg);
}

std::shared_ptr<class relation_io_map> const& step::relation_io_map() const noexcept {
    return relation_io_map_;
}

void step::io_exchange_map(std::shared_ptr<class io_exchange_map> arg) noexcept {
    io_exchange_map_ = std::move(arg);
}

std::shared_ptr<class io_exchange_map> const& step::io_exchange_map() const noexcept {
    return io_exchange_map_;
}
}
