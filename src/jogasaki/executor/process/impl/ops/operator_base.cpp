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
#include "operator_base.h"

#include <utility>

#include <jogasaki/executor/process/processor_info.h>

namespace jogasaki::executor::process::impl::ops {

operator_base::operator_base(
    operator_base::operator_index_type index,
    processor_info const&info,
    operator_base::block_index_type block_index,
    variable_table_info const* input_variable_info,
    variable_table_info const* output_variable_info
) noexcept:
    index_(index),
    processor_info_(std::addressof(info)),
    block_index_(block_index),
    input_variable_info_(
        input_variable_info != nullptr ?
            input_variable_info :
            std::addressof(processor_info_->vars_info_list()[block_index_])
    ),
    output_variable_info_(
        output_variable_info != nullptr ?
            output_variable_info :
            std::addressof(processor_info_->vars_info_list()[block_index_])
    )
{}

variable_table_info const& operator_base::block_info() const noexcept {
    return *input_variable_info_;
}

variable_table_info const& operator_base::output_variable_info() const noexcept {
    return *output_variable_info_;
}

operator_base::block_index_type operator_base::block_index() const noexcept {
    return block_index_;
}

yugawara::compiled_info const& operator_base::compiled_info() const noexcept {
    return processor_info_->compiled_info();
}

operator_base::operator_index_type operator_base::index() const noexcept {
    return index_;
}

variable_table const* operator_base::host_variables() const noexcept {
    return processor_info_->host_variables();
}

void operator_base::dump(std::string_view indent) const noexcept {
    int width = 34 > indent.length() ? 34
        - static_cast<int>(indent.length()) : 0;
    std::cerr << indent <<  "operator_base:\n"
       << indent << "  " << std::left << std::setw(width) << "index_:"
       << index_ << "\n"
       << indent << "  " << std::setw(width) << "processor_info_:"
       << std::hex << (processor_info_ ? processor_info_ : nullptr) << "\n"
       << indent << "  " << std::setw(width) << "input_variable_info_:"
       << (input_variable_info_ ? input_variable_info_ : nullptr) << "\n"
       << indent << "  " << std::setw(width) << "output_variable_info_:"
       << (output_variable_info_ ? output_variable_info_ : nullptr) << std::endl;
}

record_operator::record_operator(
    operator_base::operator_index_type index,
    processor_info const&info,
    operator_base::block_index_type block_index,
    variable_table_info const* input_variable_info,
    variable_table_info const* output_variable_info
) noexcept:
    operator_base(
        index,
        info,
        block_index,
        input_variable_info,
        output_variable_info
    )
{}

group_operator::group_operator(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index
) noexcept:
    operator_base(
        index,
        info,
        block_index
    )
{}

}
