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
#include <tateyama/proto/kvs/data.pb.h>

namespace jogasaki::api::kvsservice {

class mapped_record {
public:
    mapped_record() = default;

    explicit mapped_record(tateyama::proto::kvs::data::Record const &record);
    const tateyama::proto::kvs::data::Value *get_value(std::string_view column);
private:
    std::unordered_map<std::string, tateyama::proto::kvs::data::Value const&> map_ {};
};
}
