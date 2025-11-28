/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <takatori/util/downcast.h>
#include <takatori/util/fail.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/environment.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/field_type.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/record.h>
#include <jogasaki/api/record_meta.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/result_set_iterator.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/add_test_tables.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/tables.h>

#include "../common/temporary_folder.h"

DEFINE_string(location, "", "specify the database directory. Pass TMP to use temporary directory.");

namespace jogasaki::client_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;

using takatori::util::fail;
using takatori::util::unsafe_downcast;

static bool prepare_data(api::database& db) {
    std::string insert_warehouse{"INSERT INTO WAREHOUSE (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES (1, 'fogereb', 'byqosjahzgrvmmmpglb', 'kezsiaxnywrh', 'jisagjxblbmp', 'ps', '694764299', 0.12, 3000000.00)"};
    std::string insert_customer{ "INSERT INTO CUSTOMER (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_data, c_ytd_payment, c_payment_cnt, c_delivery_cnt)  VALUES (1, 1, 1, 'pmdeqxrbgs', 'OE', 'BARBARBAR', 'zlaoknusaxfhasce', 'sagjvpdsyzbhsvnhwzxe', 'adftkgtros', 'qd', '827402212', '8700969702524002', '1973-12-12', 'BC', 50000.00, 0.05, -9.99, 'posxrsroejldsyoyirjofkqsycnbjoalxfkgipoogepnuwmagaxcopincpbfhwercrohqxygjjxhamineoraxkzrirkafmmjkcbkafvnqfzonsdcccijdzqlbywgcgbovpmmjcapfmfqbjnfejaqmhqqtxjayvowuujxqmzvisjghpjpynbamdhvvjncvgzstpvqeeakdpwkjmircrfysmwbbbkzbzefldktqfeubcbcjgdjsjtkcomuhqdazqmgpukiyawmqgyzkciwrxfswnegkrofklawoxypehzzztouvokzhshawbbdkasynuixskxmauxuapnkemytcrchqhvjqhntkvkmgezotza', 10.00, 1, 0)"};

    std::unique_ptr<api::executable_statement> p1{};
    std::unique_ptr<api::executable_statement> p2{};
    if(auto rc = db.create_executable(insert_warehouse, p1); rc != status::ok) {
        return false;
    }
    if(auto rc = db.create_executable(insert_customer, p2); rc != status::ok) {
        return false;
    }

    auto tx = utils::create_transaction(db);
    if(auto rc = tx->execute(*p1); rc != status::ok) {
        tx->abort();
        return false;
    }
    if(auto rc = tx->execute(*p2); rc != status::ok) {
        tx->abort();
        return false;
    }
    tx->commit();
    return true;
}

static void report_meta(api::record_meta const& meta) {
    for(std::size_t i=0, n=meta.field_count(); i < n; ++i) {
        auto& f = meta.at(i);
        LOG(INFO) << "Field " << i << " : " << f.kind();
    }
}

static void report_record(api::record_meta const& meta, api::record const& rec) {
    std::stringstream ss{};
    for(std::size_t i=0, n=meta.field_count(); i < n; ++i) {
        auto& f = meta.at(i);
        using kind = api::field_type_kind;
        switch(f.kind()) {
            case kind::int4: ss << rec.get_int4(i); break;
            case kind::int8: ss << rec.get_int8(i); break;
            case kind::float4: ss << rec.get_float4(i); break;
            case kind::float8: ss << rec.get_float8(i); break;
            case kind::character: ss << rec.get_character(i); break;
            default:
                // other types are not yet supported
                fail();
        }
        if(i != n-1) {
            ss << ", ";
        }
    }
    LOG(INFO) << ss.str();
}
static bool query(api::database& db) {
    std::string select{
        "SELECT w_tax, c_discount, c_last, c_credit FROM WAREHOUSE, CUSTOMER "
        "WHERE w_id = :w_id "
        "AND c_w_id = w_id AND "
        "c_d_id = :c_d_id AND "
        "c_id = :c_id "
    };
    api::statement_handle p{};
    if(auto rc = db.prepare(select, p); rc != status::ok) {
        return false;
    }

    auto ps = api::create_parameter_set();
    ps->set_int8("w_id", 1);
    ps->set_int8("c_d_id", 1);
    ps->set_int8("c_id", 1);

    std::unique_ptr<api::executable_statement> e{};
    if(auto rc = db.resolve(p, std::shared_ptr{std::move(ps)}, e); rc != status::ok) {
        return false;
    }

    if(auto rc = db.explain(*e, std::cout); rc != status::ok) {
        return false;
    }

    auto tx = utils::create_transaction(db);
    std::unique_ptr<api::result_set> rs{};
    if(auto rc = tx->execute(*e, rs); rc != status::ok) {
        return false;
    }

    report_meta(*rs->meta());

    auto it = rs->iterator();
    while(it->has_next()) {
        auto* record = it->next();
        report_record(*rs->meta(), *record);
    }
    tx->commit();
    if(auto rc = db.destroy_statement(p); rc != status::ok) {
        return false;
    }
    rs->close();
    return true;
}

static bool run() {
    auto env = jogasaki::api::create_environment();
    env->initialize();
    auto cfg = std::make_shared<configuration>();
    jogasaki::common_cli::temporary_folder dir{};
    if (FLAGS_location == "TMP") {
        dir.prepare();
        cfg->db_location(dir.path());
    } else {
        cfg->db_location(std::string(FLAGS_location));
    }
    auto db = jogasaki::api::create_database(cfg);
    db->start();
    utils::add_benchmark_tables();
    if(auto res = prepare_data(*db); !res) {
        db->stop();
        return false;
    }
    if(auto res = query(*db); !res) {
        db->stop();
        return false;
    }
    db->stop();
    dir.clean();
    return true;
}

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    gflags::SetUsageMessage("client cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    jogasaki::client_cli::run();  // NOLINT
    return 0;
}
