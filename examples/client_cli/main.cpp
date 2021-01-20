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

#include <jogasaki/api/database.h>
#include <jogasaki/api/environment.h>
#include <jogasaki/api/result_set.h>

namespace jogasaki::client_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;

static int run() {
    auto env = jogasaki::api::create_environment();
    env->initialize();
    auto cfg = std::make_shared<configuration>();
    cfg->prepare_benchmark_tables(true);
    auto db = jogasaki::api::create_database(cfg);
    db->start();

    std::unique_ptr<api::result_set> rs{};
    std::string insert_warehouse{"INSERT INTO WAREHOUSE (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES (1, 'fogereb', 'byqosjahzgrvmmmpglb', 'kezsiaxnywrh', 'jisagjxblbmp', 'ps', '694764299', 0.12, 3000000.00)"};
    std::string insert_customer{ "INSERT INTO CUSTOMER (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_data, c_ytd_payment, c_payment_cnt, c_delivery_cnt)  VALUES (1, 1, 1, 'pmdeqxrbgs', 'OE', 'BARBARBAR', 'zlaoknusaxfhasce', 'sagjvpdsyzbhsvnhwzxe', 'adftkgtros', 'qd', '827402212', '8700969702524002', '1973-12-12', 'BC', 50000.00, 0.05, -9.99, 'posxrsroejldsyoyirjofkqsycnbjoalxfkgipoogepnuwmagaxcopincpbfhwercrohqxygjjxhamineoraxkzrirkafmmjkcbkafvnqfzonsdcccijdzqlbywgcgbovpmmjcapfmfqbjnfejaqmhqqtxjayvowuujxqmzvisjghpjpynbamdhvvjncvgzstpvqeeakdpwkjmircrfysmwbbbkzbzefldktqfeubcbcjgdjsjtkcomuhqdazqmgpukiyawmqgyzkciwrxfswnegkrofklawoxypehzzztouvokzhshawbbdkasynuixskxmauxuapnkemytcrchqhvjqhntkvkmgezotza', 10.00, 1, 0)"};
    std::string select{
        "SELECT w_tax, c_discount, c_last, c_credit FROM WAREHOUSE, CUSTOMER "
        "WHERE w_id = 1 "
        "AND c_w_id = w_id AND "
        "c_d_id = 1 AND "
        "c_id = 1 "
    };
    std::string sql {"select * from CUSTOMER c JOIN WAREHOUSE w ON c.c_w_id = w.w_id"};
    if(auto res = db->execute(insert_warehouse); !res) {
        db->stop();
        return 1;
    }
    if(auto res = db->execute(insert_customer); !res) {
        db->stop();
        return 1;
    }
    if(auto res = db->execute(sql, rs); !res || !rs) {
        db->stop();
        return 1;
    }
    auto it = rs->iterator();
    while(it->has_next()) {
        auto* record = it->next();
        std::stringstream ss{};
        ss << *record;
        LOG(INFO) << ss.str();
    }
    rs->close();
    db->stop();
    return 0;
}

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    gflags::SetUsageMessage("client cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    try {
        jogasaki::client_cli::run();  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
