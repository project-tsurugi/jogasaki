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

#include <tateyama/proto/kvs/data.pb.h>
#include <yugawara/yugawara/storage/column.h>

namespace jogasaki::api::kvsservice {

class column_data {
public:
    column_data(yugawara::storage::column const *column, tateyama::proto::kvs::data::Value const *value) :
        column_(column), value_(value) {  }

    [[nodiscard]] yugawara::storage::column const *column() const noexcept {
        return column_;
    }

    [[nodiscard]] tateyama::proto::kvs::data::Value const *value() const noexcept {
        return value_;
    }
private:
    yugawara::storage::column const *column_{};
    tateyama::proto::kvs::data::Value const *value_{};
};
}
