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

#include <jogasaki/status.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include "search_key_field_info.h"

namespace jogasaki::executor::process::impl::ops::details {

status encode_key(
    std::vector<details::search_key_field_info> const& keys,
    executor::process::impl::variable_table& input_variables,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& out,
    std::size_t& length
);

}

