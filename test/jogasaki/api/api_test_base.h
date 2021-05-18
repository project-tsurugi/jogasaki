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

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/mock/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class api_test_base  {
protected:
    virtual bool to_explain() = 0;

public:
    void explain(api::executable_statement& stmt) {
        if (to_explain()) {
            db_->explain(stmt, std::cout);
            std::cout << std::endl;
        }
    }

    void execute_query(
        std::string_view query,
        api::parameter_set const& params,
        api::transaction& tx,
        std::vector<mock::basic_record>& out
    ) {
        std::unique_ptr<api::prepared_statement> prepared{};
        ASSERT_EQ(status::ok,db_->prepare(query, prepared));

        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->resolve(*prepared, params, stmt));
        explain(*stmt);
        std::unique_ptr<api::result_set> rs{};
        ASSERT_EQ(status::ok, tx.execute(*stmt, rs));
        ASSERT_TRUE(rs);
        auto it = rs->iterator();
        while(it->has_next()) {
            auto* record = it->next();
            std::stringstream ss{};
            ss << *record;
            auto* rec_impl = unsafe_downcast<api::impl::record>(record);
            auto* meta_impl = unsafe_downcast<api::impl::record_meta>(rs->meta());
            out.emplace_back(rec_impl->ref(), meta_impl->meta());
            LOG(INFO) << ss.str();
        }
        rs->close();
    }

    void execute_query(
        std::string_view query,
        api::parameter_set const& params,
        std::vector<mock::basic_record>& out
    ) {
        auto tx = db_->create_transaction();
        execute_query(query, params, *tx, out);
        tx->commit();
    }
    void execute_query(
        std::string_view query,
        api::transaction& tx,
        std::vector<mock::basic_record>& out
    ) {
        api::impl::parameter_set params{};
        execute_query(query, params, tx, out);
    }
    void execute_query(
        std::string_view query,
        std::vector<mock::basic_record>& out
    ) {
        api::impl::parameter_set params{};
        execute_query(query, params, out);
    }

    void execute_statement(
        std::string_view query,
        api::parameter_set const& params,
        api::transaction& tx
    ) {
        std::unique_ptr<api::prepared_statement> prepared{};
        ASSERT_EQ(status::ok,db_->prepare(query, prepared));

        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->resolve(*prepared, params, stmt));
        explain(*stmt);
        ASSERT_EQ(status::ok, tx.execute(*stmt));
    }
    void execute_statement(
        std::string_view query,
        api::parameter_set const& params
    ) {
        auto tx = db_->create_transaction();
        execute_statement(query, params, *tx);
        tx->commit();
    }
    void execute_statement(
        std::string_view query,
        api::transaction& tx
    ) {
        api::impl::parameter_set params{};
        execute_statement(query, params, tx);
    }

    void execute_statement(
        std::string_view query
    ) {
        api::impl::parameter_set params{};
        execute_statement(query, params);
    }

    void resolve(std::string& query, std::string_view place_holder, std::string value) {
        query = std::regex_replace(query, std::regex(std::string(place_holder)), value);
    }

    std::unique_ptr<jogasaki::api::database> db_;
};

}
