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
#include "metadata_store.h"

#include <takatori/util/exception.h>

#include <jogasaki/data/any.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/executor/sequence/exception.h>

namespace jogasaki::executor::sequence {

using kind = meta::field_type_kind;
using takatori::util::throw_exception;

metadata_store::metadata_store() = default;
metadata_store::~metadata_store() = default;

void metadata_store::put(std::size_t def_id, std::size_t id) {
    data::aligned_buffer key_buf{10};
    data::aligned_buffer val_buf{10};
    kvs::writable_stream key{key_buf.data(), key_buf.capacity()};
    kvs::writable_stream value{val_buf.data(), val_buf.capacity()};
    data::any k{std::in_place_type<std::int64_t>, def_id};
    data::any v{std::in_place_type<std::int64_t>, id};
    // no storage spec because field type is fixed
    if(auto res = kvs::encode(k, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_key_ascending, key);
        res != status::ok) {
        (void) tx_->abort();
        throw_exception(exception{res, "encode failed"});
    }
    if(auto res = kvs::encode_nullable(v, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_value, value);
        res != status::ok) {
        (void) tx_->abort();
        throw_exception(exception{res, "encode_nullable failed"});
    }
    if (auto res = stg_->content_put(
            *tx_,
            {key.data(), key.size()},
            {value.data(), value.size()}
        ); res != status::ok) {
        throw_exception(exception{res, "writing sequence metadata to system storage failed"});
    }
}

std::tuple<sequence_definition_id, sequence_id, bool> read_entry(
    std::unique_ptr<kvs::iterator>& it,
    kvs::transaction& tx
) {
    std::string_view k{};
    std::string_view v{};
    if (auto r = it->read_key(k); r != status::ok) {
        if(r == status::not_found) {
            return {{}, {}, false};
        }
        (void) tx.abort();
        throw_exception(exception{r});
    }
    if (auto r = it->read_value(v); r != status::ok) {
        if(r == status::not_found) {
            return {{}, {}, false};
        }
        (void) tx.abort();
        throw_exception(exception{r});
    }
    kvs::readable_stream key{k.data(), k.size()};
    kvs::readable_stream value{v.data(), v.size()};
    data::any dest{};
    // no storage spec because field type is fixed
    if(auto res = kvs::decode(key, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_key_ascending, dest);
        res != status::ok) {
        (void) tx.abort();
        throw_exception(exception{res});
    }
    sequence_definition_id def_id{};
    sequence_id id{};
    def_id = dest.to<std::int64_t>();
    if(auto res = kvs::decode_nullable(value, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_value, dest);
        res != status::ok) {
        (void) tx.abort();
        throw_exception(exception{res});
    }
    id = dest.to<std::int64_t>();
    return {def_id, id, true};
}

void metadata_store::scan(metadata_store::scan_consumer_type const& consumer) {
    std::unique_ptr<kvs::iterator> it{};
    if(auto res = stg_->content_scan(
            *tx_,
            "",
            kvs::end_point_kind::unbound,
            "",
            kvs::end_point_kind::unbound,
            it
        );
        res != status::ok || !it) {
        throw_exception(exception{res, "scan failed"});
    }
    while(status::ok == it->next()) {
        auto [def_id, seq_id, found] = read_entry(it, *tx_);
        if (! found) continue;
        consumer(def_id, seq_id);
    }
}

void metadata_store::find_next_empty_def_id(std::size_t& def_id) {
    std::size_t not_used = 0;
    scan([&not_used](std::size_t def_id, std::size_t id){
        (void) id;
        if(def_id <= not_used) {
            not_used = def_id + 1;
        }
    });
    def_id = not_used;
}

bool metadata_store::remove(std::size_t def_id) {
    data::aligned_buffer key_buf{10};
    kvs::writable_stream key{key_buf.data(), key_buf.capacity()};
    data::any k{std::in_place_type<std::int64_t>, def_id};
    // no storage spec because field type is fixed
    if(auto res = kvs::encode(k, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_key_ascending, key); res != status::ok) {
        (void) tx_->abort();
        throw_exception(exception{res, "encode failed"});
    }
    if (auto res = stg_->content_delete(*tx_, {key.data(), key.size()}); res != status::ok) {
        if(res == status::not_found) {
            return false;
        }
        throw_exception(exception{res, "remove sequence def_id failed"});
    }
    return true;
}

metadata_store::metadata_store(kvs::transaction& tx) :
    tx_(std::addressof(tx))
{
    stg_ = tx.database()->get_or_create_storage(system_sequences_name);
}

std::size_t metadata_store::size() {
    std::size_t ret{};
    scan([&ret] (std::size_t, std::size_t) {
        ++ret;
    });
    return ret;
}


}
