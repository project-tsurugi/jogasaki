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

#include <jogasaki/api/field_type_kind.h>

#include "common.pb.h"

namespace jogasaki::utils {

[[nodiscard]] inline api::field_type_kind type_for(::common::DataType type) {
    switch(type) {
        case ::common::DataType::INT4: return jogasaki::api::field_type_kind::int4;
        case ::common::DataType::INT8: return jogasaki::api::field_type_kind::int8;
        case ::common::DataType::FLOAT4: return jogasaki::api::field_type_kind::float4;
        case ::common::DataType::FLOAT8: return jogasaki::api::field_type_kind::float8;
        case ::common::DataType::CHARACTER: return jogasaki::api::field_type_kind::character;
        default:
            std::abort();
    }
}

}