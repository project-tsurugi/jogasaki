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

#include "mapped_columns.h"

namespace jogasaki::api::kvsservice {

mapped_columns::mapped_columns(yugawara::storage::column_list_view columns) {
    for (auto &col : columns) {
        map_[std::string{col.simple_name()}] = &col;
    }
}

const yugawara::storage::column *mapped_columns::get_column(std::string_view name) {
    auto it = map_.find(std::string{name});
    if (it != map_.end()) {
        return it->second;
    }
    return nullptr;
}
}
