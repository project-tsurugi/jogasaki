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

#include <jogasaki/api/field_type_kind.h>

#include "jogasaki/proto/sql/common.pb.h"

namespace jogasaki::utils {

namespace sql = proto::sql;

[[nodiscard]] inline api::field_type_kind type_for(sql::common::AtomType type) {
    switch(type) {
        case sql::common::AtomType::BOOLEAN: return jogasaki::api::field_type_kind::boolean;
        case sql::common::AtomType::INT4: return jogasaki::api::field_type_kind::int4;
        case sql::common::AtomType::INT8: return jogasaki::api::field_type_kind::int8;
        case sql::common::AtomType::FLOAT4: return jogasaki::api::field_type_kind::float4;
        case sql::common::AtomType::FLOAT8: return jogasaki::api::field_type_kind::float8;
        case sql::common::AtomType::DECIMAL: return jogasaki::api::field_type_kind::decimal;
        case sql::common::AtomType::CHARACTER: return jogasaki::api::field_type_kind::character;
        case sql::common::AtomType::OCTET: return jogasaki::api::field_type_kind::octet;
        case sql::common::AtomType::DATE: return jogasaki::api::field_type_kind::date;
        case sql::common::AtomType::TIME_OF_DAY: return jogasaki::api::field_type_kind::time_of_day;
        case sql::common::AtomType::TIME_OF_DAY_WITH_TIME_ZONE: return jogasaki::api::field_type_kind::time_of_day;
        case sql::common::AtomType::TIME_POINT: return jogasaki::api::field_type_kind::time_point;
        case sql::common::AtomType::TIME_POINT_WITH_TIME_ZONE: return jogasaki::api::field_type_kind::time_point;
        default:
            std::abort();
    }
}

}  // namespace jogasaki::utils
