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

#include <tateyama/proto/kvs/data.pb.h>
#include <yugawara/yugawara/storage/table.h>
#include <yugawara/yugawara/storage/provider.h>

#include "column_data.h"

namespace jogasaki::api::kvsservice {

class record_columns {
public:
    record_columns(std::shared_ptr<yugawara::storage::table const> &table,
                   tateyama::proto::kvs::data::Record const &record,
                   bool only_keys);

    [[nodiscard]] std::shared_ptr<yugawara::storage::table const> & table() const noexcept {
        return table_;
    }
    [[nodiscard]] std::vector<column_data> const &primary_keys() const noexcept {
        return primary_keys_;
    }
    [[nodiscard]] std::vector<column_data> const &values() const noexcept {
        return values_;
    }
    [[nodiscard]] tateyama::proto::kvs::data::Record const &record() const noexcept {
        return record_;
    }
private:
    tateyama::proto::kvs::data::Record const &record_;
    std::shared_ptr<yugawara::storage::table const> &table_;
    std::vector<column_data> primary_keys_{};
    std::vector<column_data> values_{};
};

}
