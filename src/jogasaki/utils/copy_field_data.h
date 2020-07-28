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

#include <cstddef>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/field_type.h>

namespace jogasaki::utils {

void copy_field(
    meta::field_type const& type,
    accessor::record_ref target,
    std::size_t target_offset,
    accessor::record_ref source,
    std::size_t source_offset
);

}

