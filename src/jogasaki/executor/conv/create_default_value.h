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
#include <takatori/value/data.h>

#include <jogasaki/data/any.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>

namespace jogasaki::executor::conv {

/**
 * @brief create any object containing the default value converted (if necessary) to the given type
 * @param value the value to convert
 * @param type the target type of the conversion
 * @param resource the memory resource used for the conversion
 * @return the any object containing the value of type corresponding to `type`
 * @return error any object when conversion fails
*/
data::any create_immediate_default_value(
    takatori::value::data const& value,
    takatori::type::data const& type,
    memory::lifo_paged_memory_resource* resource
);

}  // namespace jogasaki::executor::conv
