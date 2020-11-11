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

static void load_data(kvs::database* db, std::string_view storage_name, std::size_t records_per_partition, bool sequential_data) {
    auto tx = db->create_transaction();
    auto stg = db->get_storage(storage_name);
    if (! stg) {
        stg = db->create_storage(storage_name);
    }

    std::string key_buf(100, '\0');
    std::string val_buf(100, '\0');
    kvs::stream key_stream{key_buf};
    kvs::stream val_stream{val_buf};

    using key_record = jogasaki::mock::basic_record<kind::int8>;
    using value_record = jogasaki::mock::basic_record<kind::float8>;
    auto key_meta = key_record{}.record_meta();
    auto val_meta = value_record{}.record_meta();

    utils::xorshift_random64 rnd{};
    for(std::size_t i=0; i < records_per_partition; ++i) {
        key_record key_rec{key_meta, static_cast<std::int64_t>(sequential_data ? i : rnd())};
        kvs::encode(key_rec.ref(), key_meta->value_offset(0), key_meta->at(0), key_stream);
        value_record val_rec{val_meta, static_cast<double>(sequential_data ? i*10 : rnd())};
        kvs::encode(val_rec.ref(), val_meta->value_offset(0), val_meta->at(0), val_stream);
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
    load_data(db_impl->kvs_db().get(), "I0", 100, true);

    auto rs = db.execute(sql);
    auto it = rs->begin();
    while(it != rs->end()) {
        auto record = it.ref();
        LOG(INFO) << "C0: " << record.get_value<std::int64_t>(0) << " C1: " << record.get_value<double>(8); //FIXME use record_meta
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
