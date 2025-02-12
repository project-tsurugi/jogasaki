/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "mirror_container.h"

#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/plan/statement_work_level.h>

namespace jogasaki::plan {

void mirror_container::set(mirror_container::step_index index, mirror_container::variable_definition def) noexcept {
    variable_definitions_[index] = std::move(def);
}

mirror_container::variable_definition const& mirror_container::at(mirror_container::step_index index) const noexcept {
    return variable_definitions_.at(index);
}

void mirror_container::host_variable_info(
    std::shared_ptr<executor::process::impl::variable_table_info> host_variable_info
) noexcept {
    host_variable_info_ = std::move(host_variable_info);
}

std::shared_ptr<executor::process::impl::variable_table_info> const&
mirror_container::host_variable_info() const noexcept {
    return host_variable_info_;
}

void mirror_container::external_writer_meta(std::shared_ptr<meta::external_record_meta> meta) noexcept {
    external_writer_meta_ = std::move(meta);
}

std::shared_ptr<meta::external_record_meta> const& mirror_container::external_writer_meta() const noexcept {
    return external_writer_meta_;
}

statement_work_level& mirror_container::work_level() noexcept {
    return work_level_;
}

void mirror_container::set_partitions(std::size_t size) noexcept {
    partitions_ = size;
}
std::size_t mirror_container::get_partitions() const noexcept {
    return partitions_;
}

} // namespace jogasaki::plan
