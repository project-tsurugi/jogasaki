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

#include <takatori/type/data.h>

#include <jogasaki/data/any.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>

namespace jogasaki::executor::conv {

/**
 * @brief conduct the assignment conversion
 * @details convert the input value of source type to target type
 * @param source_type the source type of the conversion
 * @param target_type the target type of the conversion
 * @param in the input value to convert
 * @param out the output value of the conversion
 * @param ctx the request context
 * @param resource the memory resource used for the conversion and output data
 * @warning output data can possibly be allocated in `resource` and caller is responsible to rewind the resource
*/
status conduct_assignment_conversion(
    takatori::type::data const& source_type,
    takatori::type::data const& target_type,
    data::any const& in,
    data::any& out,
    request_context& ctx,
    memory::lifo_paged_memory_resource* resource
);

/**
 * @brief conduct the unifying conversion
 * @details convert the input value of source type to target type
 * @param source_type the source type of the conversion
 * @param target_type the target type of the conversion
 * @param in the input value to convert
 * @param out the output value of the conversion
 * @param resource the memory resource used for the conversion and output data
 * @warning output data can possibly be allocated in `resource` and caller is responsible to rewind the resource
*/
status conduct_unifying_conversion(
    takatori::type::data const& source_type,
    takatori::type::data const& target_type,
    data::any const& in,
    data::any& out,
    memory::lifo_paged_memory_resource* resource
);

/**
 * @brief check if the conversion is required
 * @details this is the common function to check if source and target types are different
*/
bool to_require_conversion(
    takatori::type::data const& source_type,
    takatori::type::data const& target_type
);

}  // namespace jogasaki::executor::conv
