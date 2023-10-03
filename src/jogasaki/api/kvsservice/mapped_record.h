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

#include <unordered_map>
#include <tateyama/proto/kvs/data.pb.h>

namespace jogasaki::api::kvsservice {

/**
 * @brief the wrapper to get a column value by a column name.
 */
class mapped_record {
public:
    mapped_record() = default;

    /**
     * @brief create new object
     * @param record the record
     */
    explicit mapped_record(tateyama::proto::kvs::data::Record const &record);

    /**
     * @brief get the value of specified column name
     * @param column the name of the column
     * @return the value of the specified column
     * @return nullptr if the column not found
     */
    const tateyama::proto::kvs::data::Value *get_value(std::string_view column);
private:
    std::unordered_map<std::string, tateyama::proto::kvs::data::Value const&> map_ {};
};
}
