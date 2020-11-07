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
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

operator_base::operator_base(operator_base::operator_index_type index, const processor_info &info,
    operator_base::block_index_type block_index) noexcept:
    index_(index),
    processor_info_(std::addressof(info)),
    block_index_(block_index)
{}

block_scope_info const &operator_base::block_info() const noexcept {
    return processor_info_->scopes_info()[block_index_];
}

operator_base::block_index_type operator_base::block_index() const noexcept {
    return block_index_;
}

std::vector<block_scope_info> const &operator_base::blocks() const noexcept {
    return processor_info_->scopes_info();
}

yugawara::compiled_info const &operator_base::compiled_info() const noexcept {
    return processor_info_->compiled_info();
}

operator_base::operator_index_type operator_base::index() const noexcept {
    return index_;
}

record_operator::record_operator(operator_base::operator_index_type index, const processor_info &info,
    operator_base::block_index_type block_index) noexcept:
    operator_base(
        index,
        info,
        block_index
    ) {}

group_operator::group_operator(operator_base::operator_index_type index, const processor_info &info,
    operator_base::block_index_type block_index) noexcept:
    operator_base(
        index,
        info,
        block_index
    ) {}
}
