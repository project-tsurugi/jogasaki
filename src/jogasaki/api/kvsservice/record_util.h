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
#include <sharksfin/api.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/kvsservice/status.h>

namespace jogasaki::api::kvsservice {

class record_util {
public:
    record_util() = default;

    explicit record_util(jogasaki::api::database* db);

    record_util(record_util const &other) = delete;
    record_util &operator=(record_util const &other) = delete;
    record_util(record_util &&other) noexcept = delete;
    record_util &operator=(record_util &&other) noexcept = delete;

    ~record_util() = default;

    status check_put_record(std::string_view table_name, tateyama::proto::kvs::data::Record const &record);

    status prepare_put_record(std::string_view table_name, tateyama::proto::kvs::data::Record const &record,
                            sharksfin::Slice &key, sharksfin::Slice &value);

private:
    jogasaki::api::impl::database* db_;
};
}
