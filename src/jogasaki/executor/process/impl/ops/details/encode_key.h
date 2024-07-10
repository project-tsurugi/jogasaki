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

#include <cstddef>
#include <vector>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/status.h>

#include "search_key_field_info.h"

namespace jogasaki::executor::process::impl::ops::details {

/**
 * @brief evaluate the search key and encode
 * @details evaluate the search key and encode it so that it can be used for search
 * @param keys the key fields to be evaluated
 * @param input_variables the variables to be used for evaluation
 * @param resource the memory resource
 * @param out the buffer to store the encoded key
 * @param length the length of the encoded key
 * @param message the error message filled only when the return value is not status::ok and message is available
 * @return status::ok when successful
 * @return status::err_integrity_constraint_violation when evaluation results in null where it is not allowed
 * @return status::err_type_mismatch if the type of the evaluated value does not match the expected type
 * @return status::err_expression_evaluation_failure any other evaluation failure
 */
status encode_key(
    std::vector<details::search_key_field_info> const& keys,
    executor::process::impl::variable_table& input_variables,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& out,
    std::size_t& length,
    std::string& message
);

}  // namespace jogasaki::executor::process::impl::ops::details
