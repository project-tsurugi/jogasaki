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
#include "io_info.h"

#include <utility>

#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;

input_info::input_info(
    maybe_shared_ptr<meta::record_meta> meta,
    meta::variable_order column_order
) :
    record_meta_(std::move(meta)),
    column_order_(std::move(column_order))
{}

input_info::input_info(
    maybe_shared_ptr<meta::group_meta> meta,
    meta::variable_order column_order
) :
    group_meta_(std::move(meta)),
    column_order_(std::move(column_order)),
    for_group_(true)
{}

maybe_shared_ptr<meta::record_meta> const& input_info::record_meta() const noexcept {
    return record_meta_;
}

maybe_shared_ptr<meta::group_meta> const& input_info::group_meta() const noexcept {
    return group_meta_;
}

meta::variable_order const& input_info::column_order() const noexcept {
    return column_order_;
}

bool input_info::is_group_input() const noexcept {
    return for_group_;
}

output_info::output_info(
    maybe_shared_ptr<meta::record_meta> meta,
    meta::variable_order column_order
) :
    meta_(std::move(meta)),
    column_order_(std::move(column_order))
{}

maybe_shared_ptr<meta::record_meta> const& output_info::meta() const noexcept {
    return meta_;
}

meta::variable_order const& output_info::column_order() const noexcept {
    return column_order_;
}

external_output_info::external_output_info(
    maybe_shared_ptr<meta::record_meta> meta,
    meta::variable_order column_order
) :
    meta_(std::move(meta)),
    column_order_(std::move(column_order))
{}

meta::record_meta const& external_output_info::meta() const noexcept {
    return *meta_;
}

meta::variable_order const& external_output_info::column_order() const noexcept {
    return column_order_;
}

io_info::io_info(
    io_info::input_entity_type inputs,
    io_info::output_entity_type outputs,
    io_info::external_output_entity_type external_outputs
) :
    inputs_(std::move(inputs)),
    outputs_(std::move(outputs)),
    external_outputs_(std::move(external_outputs))
{}

input_info const& io_info::input_at(std::size_t index) const {
    return inputs_.at(index);
}

output_info const& io_info::output_at(std::size_t index) const {
    return outputs_.at(index);
}

external_output_info const& io_info::external_output_at(std::size_t index) const {
    return external_outputs_.at(index);
}

std::size_t io_info::input_count() const noexcept {
    return inputs_.size();
}

std::size_t io_info::output_count() const noexcept {
    return outputs_.size();
}

std::size_t io_info::external_output_count() const noexcept {
    return external_outputs_.size();
}

}


