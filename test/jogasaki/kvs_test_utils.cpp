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
#include "kvs_test_utils.h"

#include <cstddef>
#include <ostream>
#include <type_traits>
#include <boost/assert.hpp>
#include <glog/logging.h>

#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/status.h>

namespace jogasaki {

using takatori::util::maybe_shared_ptr;
using takatori::util::fail;
using kind = meta::field_type_kind;


std::string kvs_test_utils::put(
    kvs::database& db,
    std::string_view storage_name,
    mock::basic_record key,
    mock::basic_record value
) {
    BOOST_ASSERT(key);  //NOLINT
    auto stg = db.get_or_create_storage(storage_name);
    auto tx = db.create_transaction();

    std::string key_buf(1000, '\0');
    std::string val_buf(1000, '\0');
    kvs::writable_stream key_stream{key_buf};
    kvs::writable_stream val_stream{val_buf};

    auto& key_meta = key.record_meta();
    for(std::size_t i=0, n=key_meta->field_count(); i < n; ++i) {
        kvs::coding_context ctx{};
        if (key_meta->nullable(i)) {

            kvs::encode_nullable(
                key.ref(), key_meta->value_offset(i), key_meta->nullity_offset(i),
                key_meta->at(i), spec_asc, ctx, key_stream);
            continue;
        }
        kvs::encode(key.ref(), key_meta->value_offset(i), key_meta->at(i), spec_asc, ctx, key_stream);
    }
    if (value) {
        auto& val_meta = value.record_meta();
        kvs::coding_context ctx{};
        for(std::size_t i=0, n=val_meta->field_count(); i < n; ++i) {
            if (val_meta->nullable(i)) {
                kvs::encode_nullable(
                    value.ref(), val_meta->value_offset(i), val_meta->nullity_offset(i),
                    val_meta->at(i), spec_val, ctx, val_stream);
                continue;
            }
            kvs::encode(value.ref(), val_meta->value_offset(i), val_meta->at(i), spec_val, ctx, val_stream);
        }
    }
    if(auto res = stg->content_put(*tx,
            std::string_view{key_buf.data(), key_stream.size()},
            std::string_view{val_buf.data(), val_stream.size()}
        ); res != status::ok) {
        LOG(ERROR) << "content_put returned: " << res;
        fail();
    }
    if(auto res = tx->commit(true); res != status::ok) {
        LOG(ERROR) << "commit returned: " << res;
        fail();
    }
    return std::string{key_buf.data(), key_stream.size()};
}

void kvs_test_utils::put_secondary(
    kvs::database& db,
    std::string_view storage_name,
    mock::basic_record key,
    std::string_view encoded_primary_key
) {
    BOOST_ASSERT(key);  //NOLINT
    auto stg = db.get_or_create_storage(storage_name);
    auto tx = db.create_transaction();

    std::string key_buf(1000, '\0');
    kvs::writable_stream key_stream{key_buf};

    auto& key_meta = key.record_meta();
    for(std::size_t i=0, n=key_meta->field_count(); i < n; ++i) {
        kvs::coding_context ctx{};
        if (key_meta->nullable(i)) {
            kvs::encode_nullable(
                key.ref(), key_meta->value_offset(i), key_meta->nullity_offset(i),
                key_meta->at(i), spec_asc, ctx, key_stream);
            continue;
        }
        kvs::encode(key.ref(), key_meta->value_offset(i), key_meta->at(i), spec_asc, ctx, key_stream);
    }
    key_stream.write(encoded_primary_key.data(), encoded_primary_key.size());
    if(auto res = stg->content_put(*tx,
            std::string_view{key_buf.data(), key_stream.size()},
            std::string_view{}
        ); res != status::ok) {
        LOG(ERROR) << "content_put returned: " << res;
        fail();
    }
    if(auto res = tx->commit(true); res != status::ok) {
        LOG(ERROR) << "commit returned: " << res;
        fail();
    }
}

void kvs_test_utils::get(
    kvs::database& db,
    std::string_view storage_name,
    mock::basic_record key_model,
    mock::basic_record value_model,
    std::vector<std::pair<mock::basic_record, mock::basic_record>>& result
) {
    auto stg = db.get_or_create_storage(storage_name);
    auto tx = db.create_transaction();

    std::unique_ptr<kvs::iterator> it{};
    if(auto res = stg->content_scan(*tx,
            "", kvs::end_point_kind::unbound,
            "", kvs::end_point_kind::unbound,
            it
        ); res != status::ok) {
        LOG(ERROR) << "content_scan returned: " << res;
        fail();
    }
    std::string key_buf(1000, '\0');
    std::string val_buf(1000, '\0');
    kvs::readable_stream key_stream{key_buf};
    kvs::readable_stream val_stream{val_buf};
    auto& key_meta = key_model.record_meta();
    auto& val_meta = value_model.record_meta();
    while(it->next() == status::ok) {
        std::string_view k{};
        std::string_view v{};
        if (auto r = it->read_key(k); r != status::ok) {
            LOG(ERROR) << "key returned: " << r;
            fail();
        }
        if (auto r = it->read_value(v); r != status::ok) {
            LOG(ERROR) << "value returned: " << r;
            fail();
        }
        std::string key_buf(k);
        std::string val_buf(v);
        kvs::readable_stream key_stream{key_buf};
        kvs::readable_stream val_stream{val_buf};

        kvs::coding_context ctx{};
        for(std::size_t i=0, n=key_meta->field_count(); i < n; ++i) {
            if (key_meta->nullable(i)) {
                kvs::decode_nullable(
                    key_stream,
                    key_meta->at(i),
                    spec_asc,
                    ctx,
                    key_model.ref(),
                    key_meta->value_offset(i),
                    key_meta->nullity_offset(i)
                );
                continue;
            }
            kvs::decode(
                key_stream,
                key_meta->at(i),
                spec_asc,
                ctx,
                key_model.ref(),
                key_meta->value_offset(i)
            );
        }
        for(std::size_t i=0, n=val_meta->field_count(); i < n; ++i) {
            if (val_meta->nullable(i)) {
                kvs::decode_nullable(
                    val_stream,
                    val_meta->at(i),
                    spec_val,
                    ctx,
                    value_model.ref(),
                    val_meta->value_offset(i),
                    val_meta->nullity_offset(i)
                );
                continue;
            }
            kvs::decode(
                val_stream,
                val_meta->at(i),
                spec_val,
                ctx,
                value_model.ref(),
                val_meta->value_offset(i)
            );
        }
        result.emplace_back(key_model, value_model);
    }
    if(auto res = tx->commit(true); res != status::ok) {
        LOG(ERROR) << "commit returned: " << res;
        fail();
    }
}

std::unique_ptr<kvs::storage> kvs_test_utils::get_storage(
    kvs::database& db,
    std::string_view name
) {
    if(auto ret = db.get_or_create_storage(name)) {
        return ret;
    }
    fail();
}

}

