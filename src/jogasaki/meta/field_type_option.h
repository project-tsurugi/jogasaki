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

#include <cstddef>
#include <type_traits>
#include <variant>

#include <jogasaki/accessor/text.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::meta {

// placeholders for optional information for types
// TODO implement for production
struct array_field_option {}; //NOLINT
struct record_field_option {};
struct row_reference_field_option {};
struct row_id_field_option {};
struct declared_field_option {};
struct extension_field_option {};

} // namespace

