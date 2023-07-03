/*
 * Copyright 2018-2023 tsurugi project.
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

#include <unordered_map>
#include <yugawara/storage/table.h>
#include <yugawara/storage/column.h>

namespace jogasaki::api::kvsservice {

class mapped_columns {
public:
    mapped_columns() = default;

    explicit mapped_columns(yugawara::storage::column_list_view columns);
    const yugawara::storage::column* get_column(std::string_view name);
private:
    std::unordered_map<std::string, yugawara::storage::column const*> map_ {};
};
}