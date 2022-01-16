/*
 * Copyright 2018-2020 tsurugi project.
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

#include <gtest/gtest.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/fail.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_utils/record.h>
#include <jogasaki/status.h>

namespace jogasaki {

using takatori::util::maybe_shared_ptr;
using takatori::util::fail;
using kind = meta::field_type_kind;

class kvs_test_utils {
public:
    static constexpr kvs::order undef = kvs::order::undefined;
    static constexpr kvs::order asc = kvs::order::ascending;
    static constexpr kvs::order desc = kvs::order::descending;

    static constexpr kvs::coding_spec spec_asc = kvs::spec_key_ascending;
    static constexpr kvs::coding_spec spec_desc = kvs::spec_key_descending;
    static constexpr kvs::coding_spec spec_val = kvs::spec_value;

    // put key/value to storage and return binary sequence for encoded key
    std::string put(
        kvs::database& db,
        std::string_view storage_name,
        mock::basic_record key,
        mock::basic_record value
    );

    // put secondary index key by concatenating secondary keys with encoded primary key
    void put_secondary(
        kvs::database& db,
        std::string_view storage_name,
        mock::basic_record key,
        std::string_view encoded_primary_key
    );
    void get(
        kvs::database& db,
        std::string_view storage_name,
        mock::basic_record key_model,
        mock::basic_record value_model,
        std::vector<std::pair<mock::basic_record, mock::basic_record>>& result
    );

    std::unique_ptr<kvs::storage> get_storage(
        kvs::database& db,
        std::string_view name
    );
};

}
