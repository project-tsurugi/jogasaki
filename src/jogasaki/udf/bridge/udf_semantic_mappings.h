/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <functional>
#include <unordered_map>

#include <takatori/type/data.h>

#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/udf/data/udf_semantic_type.h>
#include <jogasaki/udf/enum_types.h>
#include <jogasaki/udf/plugin_api.h>

namespace jogasaki::udf::bridge {

[[nodiscard]] jogasaki::meta::field_type to_field_type(jogasaki::meta::field_type_kind k);
[[nodiscard]] const std::unordered_map<plugin::udf::type_kind, std::size_t>& type_index_map();
[[nodiscard]] std::shared_ptr<takatori::type::data const> to_takatori_type(
    plugin::udf::type_kind kind);
[[nodiscard]] jogasaki::meta::field_type_kind to_meta_kind(plugin::udf::type_kind k);
[[nodiscard]] jogasaki::meta::field_type_kind to_meta_kind(
    plugin::udf::column_descriptor const& col);
} // namespace jogasaki::udf::bridge
