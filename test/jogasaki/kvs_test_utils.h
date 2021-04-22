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
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_utils/record.h>

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

    void put(
        kvs::database& db,
        std::string_view storage_name,
        mock::basic_record key,
        mock::basic_record value
    ) {
        BOOST_ASSERT(key);  //NOLINT
        auto stg = db.create_storage(storage_name);
        if (! stg) {
            stg = db.get_storage(storage_name);
        }
        auto tx = db.create_transaction();

        std::string key_buf(1000, '\0');
        std::string val_buf(1000, '\0');
        kvs::stream key_stream{key_buf};
        kvs::stream val_stream{val_buf};

        auto& key_meta = key.record_meta();
        for(std::size_t i=0, n=key_meta->field_count(); i < n; ++i) {
            kvs::encode(key.ref(), key_meta->value_offset(i), key_meta->at(i), spec_asc, key_stream);
        }
        if (value) {
            auto& val_meta = value.record_meta();
            for(std::size_t i=0, n=val_meta->field_count(); i < n; ++i) {
                kvs::encode(value.ref(), val_meta->value_offset(i), val_meta->at(i), spec_val, val_stream);
            }
        }
        if(auto res = stg->put(*tx,
                std::string_view{key_buf.data(), key_stream.length()},
                std::string_view{val_buf.data(), val_stream.length()}
            ); res != status::ok) {
            fail();
        }
        if(auto res = tx->commit(); res != status::ok) fail();
    }

};

}
