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

#include <jogasaki/api.h>
#include <jogasaki/api/environment.h>
#include <jogasaki/api/result_set.h>

namespace jogasaki::client_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;

using takatori::util::fail;

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

    auto tx = db.create_transaction();
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
    std::string sql {"select * from CUSTOMER c where c_id = :c_id"};
    std::unique_ptr<api::prepared_statement> p{};
    if(auto rc = db.prepare(sql, p); rc != status::ok) {
        return false;
    }

    auto ps = api::create_parameter_set();
    ps->set_int8("w_id", 1);
    ps->set_int8("c_d_id", 1);
    ps->set_int8("c_id", 1);

    std::unique_ptr<api::executable_statement> e{};
    if(auto rc = db.resolve(*p, *ps, e); rc != status::ok) {
        return false;
    }

    if(auto rc = db.explain(*e, std::cout); rc != status::ok) {
        return false;
    }

    auto tx = db.create_transaction();
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
    rs->close();
    return true;
}

static bool run() {
    auto env = jogasaki::api::create_environment();
    env->initialize();
    auto cfg = std::make_shared<configuration>();
    cfg->prepare_benchmark_tables(true);
    auto db = jogasaki::api::create_database(cfg);
    db->start();
    if(auto res = prepare_data(*db); !res) {
        db->stop();
        return false;
    }
    if(auto res = query(*db); !res) {
        db->stop();
        return false;
    }
    db->stop();
    return true;
}

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    gflags::SetUsageMessage("client cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    jogasaki::client_cli::run();  // NOLINT
    return 0;
}
