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

#include "mapped_record.h"

#include <utility>

#include <tateyama/proto/kvs/data.pb.h>

namespace jogasaki::api::kvsservice {

mapped_record::mapped_record(tateyama::proto::kvs::data::Record const &record) {
    for (auto i = 0; i < record.names_size(); i++) {
        map_.try_emplace(record.names(i), record.values(i));
    }
}

const tateyama::proto::kvs::data::Value *mapped_record::get_value(std::string_view column) {
    auto it = map_.find(std::string{column});
    if (it != map_.end()) {
        return &it->second;
    }
    return nullptr;
}

}

