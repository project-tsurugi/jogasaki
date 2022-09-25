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

#include <yugawara/compiled_info.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/date.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/decimal.h>
#include <takatori/descriptor/variable.h>

#include <jogasaki/constants.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_option.h>

namespace jogasaki::utils {

/**
 * @brief map takatori type to field type
 * @param type source takatori data type
 * @return mapped type
 */
[[nodiscard]] meta::field_type type_for(::takatori::type::data const& type);

/**
 * @brief retrieve the field type used for variable
 * @param info compiled info that contains the information on the target variable
 * @param var target variable
 * @return type of the variable
 */
[[nodiscard]] meta::field_type type_for(
    yugawara::compiled_info const& info,
    ::takatori::descriptor::variable const& var
);

}

