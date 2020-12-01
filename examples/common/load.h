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

#include <fstream>
#include <glog/logging.h>
#include <boost/filesystem.hpp>

#include <takatori/util/fail.h>

#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/utils/random.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/storage_dump.h>

namespace jogasaki::common_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;
using kind = meta::field_type_kind;
using takatori::util::fail;
using yugawara::storage::configurable_provider;
using namespace jogasaki::executor::process;
using namespace jogasaki::executor::process::impl;
using namespace jogasaki::executor::process::impl::expression;
using takatori::util::enum_tag_t;
using takatori::util::enum_tag;

constexpr kvs::order asc = kvs::order::ascending;
constexpr kvs::order desc = kvs::order::descending;
constexpr kvs::order undef = kvs::order::undefined;

void encode_field(
    expression::any const& a,
    meta::field_type f,
    kvs::coding_spec spec,
    bool nullable,
    kvs::stream& target
) {
    if (nullable) {
        kvs::encode_nullable(a, f, spec, target);
        return;
    }
    kvs::encode(a, f, spec, target);
}
static void fill_fields(
    meta::record_meta const& meta,
    kvs::stream& target,
    bool key,
    std::size_t record_count,
    bool sequential,
    utils::xorshift_random64& rnd,
    std::vector<bool> const& key_order_asc = {}
) {
    std::size_t field_index = 0;
    for(auto&& f: meta) {
        auto spec = key ?
            (key_order_asc[field_index] ? kvs::spec_key_ascending : kvs::spec_key_descending) :
            kvs::spec_value;
        bool nullable = meta.nullable(field_index);
        switch(f.kind()) {
            case kind::int4: {
                expression::any a{std::in_place_type<std::int32_t>, sequential ? record_count : rnd()};
                encode_field(a, meta::field_type(enum_tag<kind::int4>), spec, nullable, target);
                break;
            }
            case kind::int8: {
                expression::any a{std::in_place_type<std::int64_t>, sequential ? record_count : rnd()};
                encode_field(a, meta::field_type(enum_tag<kind::int8>), spec, nullable, target);
                break;
            }
            case kind::float4: {
                expression::any a{std::in_place_type<float>, sequential ? record_count : rnd()};
                encode_field(a, meta::field_type(enum_tag<kind::float4>), spec, nullable, target);
                break;
            }
            case kind::float8: {
                expression::any a{std::in_place_type<double>, sequential ? record_count : rnd()};
                encode_field(a, meta::field_type(enum_tag<kind::float8>), spec, nullable, target);
                break;
            }
            case kind::character: {
                char c = 'A' + (sequential ? record_count : rnd()) % 26;
                std::size_t len = 1 + (sequential ? record_count : rnd()) % 70;
                len = record_count % 2 == 1 ? len + 20 : len;
                std::string d(len, c);
                expression::any a{std::in_place_type<accessor::text>, accessor::text{d.data(), d.size()}};
                encode_field(a, meta::field_type(enum_tag<kind::character>), spec, nullable, target);
                break;
            }
            default:
                fail();
                break;
        }
        ++field_index;
    }
}

void populate_storage_data(
    kvs::database* db,
    std::shared_ptr<configurable_provider> const& provider,
    std::string_view storage_name,
    std::size_t records_per_partition,
    bool sequential_data
) {
    auto stg = db->get_storage(storage_name);
    if (! stg) {
        stg = db->create_storage(storage_name);
    }

    static std::size_t buflen = 1024;
    std::string key_buf(buflen, '\0');
    std::string val_buf(buflen, '\0');
    kvs::stream key_stream{key_buf};
    kvs::stream val_stream{val_buf};

    auto idx = provider->find_index(storage_name);

    std::vector<meta::field_type> flds{};
    boost::dynamic_bitset<std::uint64_t> nullabilities{};
    std::vector<bool> key_order_asc{};
    for(auto&& k : idx->keys()) {
        flds.emplace_back(utils::type_for(k.column().type()));
        nullabilities.push_back(k.column().criteria().nullity().nullable());
        key_order_asc.emplace_back(k.direction() == yugawara::storage::sort_direction::ascendant);
    }
    meta::record_meta key_meta{std::move(flds), std::move(nullabilities)};
    flds.clear();
    nullabilities.clear();
    for(auto&& v : idx->values()) {
        auto& c = static_cast<yugawara::storage::column const&>(v);
        flds.emplace_back(utils::type_for(c.type()));
        nullabilities.push_back(c.criteria().nullity().nullable());
    }
    meta::record_meta val_meta{std::move(flds), std::move(nullabilities)};

    static std::size_t record_per_transaction = 10000;
    std::unique_ptr<kvs::transaction> tx{};
    utils::xorshift_random64 rnd{};
    for(std::size_t i=0, n=records_per_partition; i < n; ++i) {
        if (! tx) {
            tx = db->create_transaction();
        }
        fill_fields(key_meta, key_stream, true, i, sequential_data, rnd, key_order_asc);
        fill_fields(val_meta, val_stream, false, i, sequential_data, rnd);
        if(auto res = stg->put(*tx,
                std::string_view{key_buf.data(), key_stream.length()},
                std::string_view{val_buf.data(), val_stream.length()}
            ); !res) {
            fail();
        }
        key_stream.reset();
        val_stream.reset();
        if (i == n-1 || (i != 0 && (i % record_per_transaction) == 0)) {
            if (auto res = tx->commit(); !res) {
                fail();
            }
            VLOG(2) << "committed after " << i << "-th record";
            tx.reset();
        }
    }
}

void dump_storage(std::string_view dir, kvs::database* db, std::string_view storage_name) {
    boost::filesystem::path path{boost::filesystem::current_path()};
    path /= std::string(dir);
    if (! boost::filesystem::exists(path)) {
        if (! boost::filesystem::create_directories(path)) {
            LOG(ERROR) << "creating directory failed";
        }
    }
    std::string file(storage_name);
    file += ".dat";
    path /= file;
    boost::filesystem::ofstream out{path};
    kvs::storage_dump dumper{*db};
    dumper.dump(out, storage_name, 10000);
}

void load_storage(std::string_view dir, kvs::database* db, std::string_view storage_name) {
    boost::filesystem::path path{boost::filesystem::current_path()};
    path /= std::string(dir);
    std::string file(storage_name);
    file += ".dat";
    path /= file;
    if (! boost::filesystem::exists(path)) {
        LOG(ERROR) << "File not found: " << path.string();
        fail();
    }
    boost::filesystem::ifstream in{path};
    kvs::storage_dump dumper{*db};
    dumper.load(in, storage_name, 10000);
}

} //namespace
