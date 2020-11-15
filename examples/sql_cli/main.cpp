/*
 * Copyright 2018-2019 tsurugi project.
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
#include <iostream>
#include <vector>

#include <glog/logging.h>
#include <takatori/util/fail.h>

#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/utils/random.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/database_impl.h>
#include <jogasaki/api/result_set.h>

namespace jogasaki::sql_cli {

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

static void fill_fields(meta::record_meta const& meta, kvs::stream& target, bool key, std::size_t record_count, bool sequential) {
    auto odr = key ? kvs::order::ascending : kvs::order::descending;
    static utils::xorshift_random64 rnd{};
    for(auto&& f: meta) {
        switch(f.kind()) {
            case kind::int4: {
                expression::any a{std::in_place_type<std::int32_t>, sequential ? record_count : rnd()};
                kvs::encode(a, meta::field_type(enum_tag<kind::int4>), odr, target);
                break;
            }
            case kind::int8: {
                expression::any a{std::in_place_type<std::int64_t>, sequential ? record_count : rnd()};
                kvs::encode(a, meta::field_type(enum_tag<kind::int8>), odr, target);
                break;
            }
            case kind::float4: {
                expression::any a{std::in_place_type<float>, sequential ? record_count : rnd()};
                kvs::encode(a, meta::field_type(enum_tag<kind::float4>), odr, target);
                break;
            }
            case kind::float8: {
                expression::any a{std::in_place_type<double>, sequential ? record_count : rnd()};
                kvs::encode(a, meta::field_type(enum_tag<kind::float8>), odr, target);
                break;
            }
            case kind::character: {
                char c = 'A' + (sequential ? record_count : rnd()) % 26;
                std::size_t len = 1 + (sequential ? record_count : rnd() % 70);
                len = record_count % 2 == 1 ? len + 20 : len;
                std::string d(len, c);
                expression::any a{std::in_place_type<accessor::text>, accessor::text{d.data(), d.size()}};
                kvs::encode(a, meta::field_type(enum_tag<kind::character>), odr, target);
                break;
            }
            default:
                break;
        }
    }
}
static void load_data(
    kvs::database* db,
    std::shared_ptr<configurable_provider> const& provider,
    std::string_view storage_name,
    std::size_t records_per_partition,
    bool sequential_data
) {
    auto tx = db->create_transaction();
    auto stg = db->get_storage(storage_name);
    if (! stg) {
        stg = db->create_storage(storage_name);
    }

    std::string key_buf(100, '\0');
    std::string val_buf(100, '\0');
    kvs::stream key_stream{key_buf};
    kvs::stream val_stream{val_buf};

    auto idx = provider->find_index(storage_name);

    std::vector<meta::field_type> flds{};
    for(auto&& k : idx->keys()) {
        flds.emplace_back(utils::type_for(k.column().type()));
    }
    meta::record_meta key_meta{std::move(flds), boost::dynamic_bitset<std::uint64_t>{idx->keys().size()}};
    flds.clear();
    for(auto&& v : idx->values()) {
        flds.emplace_back(utils::type_for(static_cast<yugawara::storage::column const&>(v).type()));
    }
    meta::record_meta val_meta{std::move(flds), boost::dynamic_bitset<std::uint64_t>{idx->values().size()}};

    for(std::size_t i=0; i < records_per_partition; ++i) {
        fill_fields(key_meta, key_stream, true, i, sequential_data);
        fill_fields(val_meta, val_stream, false, i, sequential_data);
        if(auto res = stg->put(*tx,
                std::string_view{key_buf.data(), key_stream.length()},
                std::string_view{val_buf.data(), val_stream.length()}
            ); !res) {
            fail();
        }
        key_stream.reset();
        val_stream.reset();
    }
    if (auto res = tx->commit(); !res) {
        fail();
    }
}

static int run(std::string_view sql) {
    if (sql.empty()) return 0;
    jogasaki::api::database db{};
    db.start();

    auto db_impl = api::database::impl::get_impl(db);
    load_data(db_impl->kvs_db().get(), db_impl->provider(), "I0", 5, true);
    load_data(db_impl->kvs_db().get(), db_impl->provider(), "I1", 5, true);

    auto rs = db.execute(sql);
    auto it = rs->begin();
    while(it != rs->end()) {
        auto record = it.ref();
        std::stringstream ss{};
        ss << record << *rs->meta();
        LOG(INFO) << ss.str();
        ++it;
    }
    rs->close();
    db.stop();
    return 0;
}

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("sql cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("sql cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 2) {
        gflags::ShowUsageWithFlags(argv[0]); // NOLINT
        return -1;
    }
    std::string_view source { argv[1] }; // NOLINT
    try {
        jogasaki::sql_cli::run(source);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
