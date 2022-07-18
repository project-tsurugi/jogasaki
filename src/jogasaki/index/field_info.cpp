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
#include "field_info.h"

namespace jogasaki::index {

field_info::field_info(
    meta::field_type type,
    bool exists,
    std::size_t offset,
    std::size_t nullity_offset,
    bool nullable,
    kvs::coding_spec spec
) :
    type_(std::move(type)),
    exists_(exists),
    offset_(offset),
    nullity_offset_(nullity_offset),
    nullable_(nullable),
    spec_(spec)
{}

}


