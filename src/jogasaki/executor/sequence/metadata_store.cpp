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
#include "metadata_store.h"

#include <takatori/util/exception.h>

#include <jogasaki/data/any.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/readable_stream.h>

namespace jogasaki::executor::sequence {

using kind = meta::field_type_kind;
using takatori::util::throw_exception;

metadata_store::metadata_store() = default;
metadata_store::~metadata_store() = default;

bool metadata_store::put(std::size_t def_id, std::size_t id) {
    data::aligned_buffer key_buf{10};
    data::aligned_buffer val_buf{10};
    kvs::writable_stream key{key_buf.data(), key_buf.capacity()};
    kvs::writable_stream value{val_buf.data(), val_buf.capacity()};
    data::any k{std::in_place_type<std::int64_t>, def_id};
    data::any v{std::in_place_type<std::int64_t>, id};
    kvs::encode(k, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_key_ascending, key);
    kvs::encode_nullable(v, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_value, value);
    if (auto res = stg_->put(
            *tx_,
            {key.data(), key.size()},
            {value.data(), value.size()}
        ); res != status::ok) {
        VLOG(log_error) << "put sequence def_id failed with error: " << res;
        return false;
    }
    return true;
}

std::tuple<sequence_definition_id, sequence_id, bool> read_entry(std::unique_ptr<kvs::iterator>& it) {
    std::string_view k{};
    std::string_view v{};
    if (auto r = it->key(k); r != status::ok) {
        if(r == status::not_found) {
            return {{}, {}, false};
        }
        throw_exception(std::logic_error{""});
    }
    if (auto r = it->value(v); r != status::ok) {
        if(r == status::not_found) {
            return {{}, {}, false};
        }
        throw_exception(std::logic_error{""});
    }
    kvs::readable_stream key{k.data(), k.size()};
    kvs::readable_stream value{v.data(), v.size()};
    data::any dest{};
    if(auto res = kvs::decode(key, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_key_ascending, dest);
        res != status::ok) {
        throw_exception(std::logic_error{""});
    }
    sequence_definition_id def_id{};
    sequence_id id{};
    def_id = dest.to<std::int64_t>();
    if(auto res = kvs::decode_nullable(value, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_value, dest);
        res != status::ok) {
        throw_exception(std::logic_error{""});
    }
    id = dest.to<std::int64_t>();
    return {def_id, id, true};
}

bool metadata_store::scan(metadata_store::scan_consumer_type const& consumer) {
    std::unique_ptr<kvs::iterator> it{};
    if(auto res = stg_->scan(
            *tx_,
            "",
            kvs::end_point_kind::unbound,
            "",
            kvs::end_point_kind::unbound,
            it
        );
        res != status::ok || !it) {
        VLOG(log_error) << "scan failed with error : " << res;
        return false;
    }
    while(status::ok == it->next()) {
        auto [def_id, seq_id, found] = read_entry(it);
        if (! found) continue;
        consumer(def_id, seq_id);
    }
    return true;
}

bool metadata_store::find_next_empty_def_id(std::size_t& def_id) {
    std::size_t not_used = 0;
    if(auto res = scan([&not_used](std::size_t def_id, std::size_t id){
            (void) id;
            if(def_id <= not_used) {
                not_used = def_id + 1;
            }
        }); ! res) {
        return res;
    }
    def_id = not_used;
    return true;
}

bool metadata_store::remove(std::size_t def_id) {
    data::aligned_buffer key_buf{10};
    kvs::writable_stream key{key_buf.data(), key_buf.capacity()};
    data::any k{std::in_place_type<std::int64_t>, def_id};
    kvs::encode(k, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_key_ascending, key);
    if (auto res = stg_->remove(*tx_, {key.data(), key.size()}); res != status::ok && res != status::not_found) {
        VLOG(log_error) << "remove sequence def_id failed with error: " << res;
        return false;
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
