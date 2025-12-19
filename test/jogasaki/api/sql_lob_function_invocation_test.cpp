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
#include <cstdint>
#include <fstream>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/type/int.h>
#include <takatori/type/lob.h>
#include <takatori/util/downcast.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>
#include <tateyama/framework/server.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/api/service/bridge.h>
#include <jogasaki/configuration.h>
#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/executor/function/field_locator.h>
#include <jogasaki/executor/function/scalar_function_info.h>
#include <jogasaki/executor/function/scalar_function_kind.h>
#include <jogasaki/executor/function/scalar_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/test_utils/create_configuration.h>
#include <jogasaki/test_utils/create_file.h>
#include <jogasaki/test_utils/data_relay_client.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/tables.h>
#include "jogasaki/executor/executor.h"

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;
using namespace tateyama;
using decimal_v = takatori::decimal::triple;
using date = takatori::datetime::date;
using time_of_day = takatori::datetime::time_of_day;
using time_point = takatori::datetime::time_point;
using executor::expr::evaluator_context;
using jogasaki::executor::expr::error;
using jogasaki::executor::expr::error_kind;
using takatori::util::sequence_view;
using takatori::util::unsafe_downcast;
using kind = meta::field_type_kind;
using namespace std::string_view_literals;
using namespace jogasaki::executor::function;
namespace t = takatori::type;
using namespace ::yugawara;

// test function invocation with blob parameter or return value
class sql_lob_function_invocation_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        temporary_.prepare();

        auto conf = test_utils::create_configuration(
            path() + "/log_location",
            path() + "/session_store"
        );
        framework::boot_mode mode = framework::boot_mode::database_server;
        server_ = std::make_unique<framework::server>(mode, conf);
        framework::add_core_components(*server_);
        auto sqlres = std::make_shared<jogasaki::api::resource::bridge>();
        server_->add_resource(sqlres);
        auto sqlsvc = std::make_shared<jogasaki::api::service::bridge>();
        server_->add_service(sqlsvc);
        server_->setup();
        server_->start();
        db_ = sqlsvc->database();
        datastore::get_datastore(true);
    }

    void TearDown() override {
        if (server_) {
            server_->shutdown();
            server_.reset();
        }
        temporary_.clean();
        global::scalar_function_repository().clear();
        if(decl_) {
            global::scalar_function_provider()->remove(*decl_);
        }
    }

    [[nodiscard]] std::string path() const {
        return temporary_.path();
    }

    std::unique_ptr<framework::server> server_{};
    std::shared_ptr<yugawara::function::declaration> decl_{}; // needed here to remove even if test fails
};

template <class T>
std::string download_lob(evaluator_context& ectx, data::any in, data_relay_client& client, std::optional<std::uint64_t> reference_tag = std::nullopt) {
    auto const& ref = in.to<T>();
    std::size_t blob_id = ref.object_id();
    auto provider = ref.provider();

    auto s = ectx.blob_session()->get_or_create();
    if (! s) {
        takatori::util::throw_exception(std::runtime_error(""));
    }
    std::uint64_t session_id = s->session_id();

    // Determine storage_id based on the data provider
    // SESSION_STORAGE_ID = 0, LIMESTONE_BLOB_STORE = 1
    std::uint64_t storage_id = (provider == lob::lob_data_provider::datastore) ? 1 : 0;

    auto tag = reference_tag ? reference_tag.value() : s->compute_tag(blob_id);
    return client.get_blob(session_id, storage_id, blob_id, tag);
}

template <class T>
data::any upload_lob(evaluator_context& ectx, std::string const& in, data_relay_client& client) {
    auto s = ectx.blob_session()->get_or_create();
    if (! s) {
        takatori::util::throw_exception(std::runtime_error(""));
    }
    std::uint64_t session_id = s->session_id();

    auto [blob_id, storage_id, tag] = client.put_blob(session_id, in);
    if (blob_id == 0) {
        takatori::util::throw_exception(
            std::runtime_error("put_blob() failed session_id:" + std::to_string(session_id))
        );
    }

    // The gRPC Put returns storage_id=0 (SESSION_STORAGE_ID)
    // but we need the blob to be accessible from the session
    return data::any{std::in_place_type<T>, T{blob_id, lob::lob_data_provider::relay_service_session}.reference_tag(tag)};
}

TEST_F(sql_lob_function_invocation_test, modify_input) {
    // function duplicates and concatenates input clob
    auto called = std::make_shared<bool>(false);
    auto id = 1000UL;  // any value to avoid conflict
    auto client = std::make_shared<data_relay_client>("localhost:52345");

    // dup function duplicates the input CLOB value
    global::scalar_function_repository().add(
        id,
        std::make_shared<scalar_function_info>(
            scalar_function_kind::mock_function_for_testing,
            [called, client](evaluator_context& ectx, sequence_view<data::any> args) -> data::any {
                *called = true;
                auto ref = args[0].to<lob::clob_reference>();
                EXPECT_TRUE(ref.lob_reference::reference_tag());
                EXPECT_NE(0, ref.lob_reference::reference_tag().value());
                LOG(INFO) << "function received blob_ref:" << ref;
                auto e = download_lob<lob::clob_reference>(ectx, args[0], *client, ref.lob_reference::reference_tag().value());
                auto concat = e + e;
                auto ret = upload_lob<lob::clob_reference>(ectx, concat, *client);
                LOG(INFO) << "function returns blob_ref:" << ret.to<lob::clob_reference>();
                return ret;
            },
            1
        )
    );
    decl_ = global::scalar_function_provider()->add({
        id,
        "dup",
        t::clob(),
        {
            t::clob(),
        },
    });
    execute_statement("create table t (c0 int primary key, c1 clob)");
    execute_statement("insert into t values (0, 'ABC'::clob)");
    auto sql = "SELECT dup(c1) FROM t";
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query(sql, *tx, result);
        ASSERT_EQ(1, result.size());

        auto v = result[0].get_value<lob::clob_reference>(0);
        EXPECT_EQ((mock::typed_nullable_record<kind::clob>(
            std::tuple{meta::clob_type()},
            {lob::clob_reference{v.object_id(), lob::lob_data_provider::datastore}},
            {false, false}
        )), result[0]);

        auto* ds = datastore::get_datastore();
        auto ret1 = ds->get_blob_file(v.object_id());
        ASSERT_TRUE(ret1);
        EXPECT_EQ("ABCABC"sv, read_file(ret1.path().string())) << ret1.path().string();
        EXPECT_EQ(status::ok, tx->commit());
    }
    EXPECT_TRUE(*called);
}

TEST_F(sql_lob_function_invocation_test, identity) {
    // verify post processing for lob return value does not affect if the return value is not stored on session
    bool called = false;
    auto id = 1000UL;  // any value to avoid conflict
    data_relay_client client{"localhost:52345"};

    // dup function duplicates the input CLOB value
    global::scalar_function_repository().add(
        id,
        std::make_shared<scalar_function_info>(
            scalar_function_kind::mock_function_for_testing,
            [&](evaluator_context&, sequence_view<data::any> args) -> data::any {
                called = true;
                auto ref = args[0].to<lob::clob_reference>();
                EXPECT_TRUE(ref.lob_reference::reference_tag());
                EXPECT_NE(0, ref.lob_reference::reference_tag().value());
                LOG(INFO) << "function receives and returns blob_ref:" << ref;
                return args[0];
            },
            1
        )
    );
    decl_ = global::scalar_function_provider()->add({
        id,
        "identity_fn",
        t::clob(),
        {
            t::clob(),
        },
    });
    execute_statement("create table t (c0 int primary key, c1 clob)");
    execute_statement("insert into t values (0, 'ABC'::clob)");
    auto sql = "SELECT identity_fn(c1) FROM t";
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query(sql, *tx, result);
        ASSERT_EQ(1, result.size());

        auto v = result[0].get_value<lob::clob_reference>(0);
        EXPECT_EQ((mock::typed_nullable_record<kind::clob>(
            std::tuple{meta::clob_type()},
            {lob::clob_reference{v.object_id(), lob::lob_data_provider::datastore}},
            {false, false}
        )), result[0]);

        auto* ds = datastore::get_datastore();
        auto ret1 = ds->get_blob_file(v.object_id());
        ASSERT_TRUE(ret1);
        EXPECT_EQ("ABC"sv, read_file(ret1.path().string())) << ret1.path().string();
        EXPECT_EQ(status::ok, tx->commit());
    }
    EXPECT_TRUE(called);
}

TEST_F(sql_lob_function_invocation_test, identity_resolved) {
    // similar to identity testcase, but use resolved blob reference
    bool called = false;
    auto id = 1000UL;  // any value to avoid conflict
    data_relay_client client{"localhost:52345"};

    // dup function duplicates the input CLOB value
    global::scalar_function_repository().add(
        id,
        std::make_shared<scalar_function_info>(
            scalar_function_kind::mock_function_for_testing,
            [&](evaluator_context&, sequence_view<data::any> args) -> data::any {
                called = true;
                auto ref = args[0].to<lob::clob_reference>();
                EXPECT_TRUE(ref.lob_reference::reference_tag());
                EXPECT_NE(0, ref.lob_reference::reference_tag().value());
                LOG(INFO) << "function receives and returns blob_ref:" << ref;
                return args[0];
            },
            1
        )
    );
    decl_ = global::scalar_function_provider()->add({
        id,
        "identity_fn",
        t::clob(),
        {
            t::clob(),
        },
    });
    execute_statement("create table t (c0 int primary key, c1 clob)");
    execute_statement("insert into t values (0, ''::clob)");
    auto sql = "SELECT identity_fn('ABC'::clob) FROM t";
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query(sql, *tx, result);
        ASSERT_EQ(1, result.size());

        auto v = result[0].get_value<lob::clob_reference>(0);
        EXPECT_EQ((mock::typed_nullable_record<kind::clob>(
            std::tuple{meta::clob_type()},
            {lob::clob_reference{v.object_id(), lob::lob_data_provider::datastore}},
            {false, false}
        )), result[0]);

        auto* ds = datastore::get_datastore();
        auto ret1 = ds->get_blob_file(v.object_id());
        ASSERT_TRUE(ret1);
        EXPECT_EQ("ABC"sv, read_file(ret1.path().string())) << ret1.path().string();
        EXPECT_EQ(status::ok, tx->commit());
    }
    EXPECT_TRUE(called);
}

bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

TEST_F(sql_lob_function_invocation_test, variety_for_lob_function_usage) {
    // verify blob_session is provided correctly to evaluator_context where blob function can be called
    auto id = 1000UL;  // any value to avoid conflict
    auto client = std::make_shared<data_relay_client>("localhost:52345");

    // dup function duplicates the input CLOB value
    global::scalar_function_repository().add(
        id,
        std::make_shared<scalar_function_info>(
            scalar_function_kind::mock_function_for_testing,
            [client](evaluator_context& ectx, sequence_view<data::any> args) -> data::any {
                auto ref = args[0].to<lob::clob_reference>();
                EXPECT_TRUE(ref.lob_reference::reference_tag());
                EXPECT_NE(0, ref.lob_reference::reference_tag().value());
                auto e = download_lob<lob::clob_reference>(ectx, args[0], *client, ref.lob_reference::reference_tag().value());
                return data::any{std::in_place_type<std::int32_t>, e.size()};
            },
            1
        )
    );
    decl_ = global::scalar_function_provider()->add({
        id,
        "clob_length",
        t::int4(),
        {
            t::clob(),
        },
    });
    execute_statement("create table t (c0 int primary key, c1 clob)");
    execute_statement("insert into t values (0, 'ABC'::clob)");
    {
        // project
        std::vector<mock::basic_record> result{};
        execute_query("SELECT clob_length(c1) FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(3)), result[0]);
    }
    {
        // filter
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t where clob_length(c1) = 3", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(0)), result[0]);
    }
    {
        // write statement
        execute_statement("create table t2 (c0 int primary key)");
        execute_statement("insert into t2 values (clob_length('ABCD'::clob))");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t2", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(4)), result[0]);
    }
    {
        // find
        execute_statement("create table t3 (c0 int primary key, c1 int)");
        execute_statement("insert into t3 values (4, 40)");

        std::string plan{};
        std::string query = "SELECT c1 FROM t3 WHERE c0 = clob_length('ABCD'::clob)";
        explain_statement(query, plan);
        ASSERT_TRUE(contains(plan, "find")) << plan;

        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(40)), result[0]);
    }
    {
        // scan
        execute_statement("create table t4 (c0 int primary key, c1 int)");
        execute_statement("insert into t4 values (8, 80)");

        std::string plan{};
        std::string query = "SELECT c1 FROM t4 WHERE c0 > clob_length('ABCD'::clob) AND c0 < clob_length('ABCD'::clob) + 10";
        explain_statement(query, plan);
        ASSERT_TRUE(contains(plan, "scan")) << plan;
        ASSERT_TRUE(! contains(plan, "filter")) << plan;

        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(80)), result[0]);
    }
    // TODO for join_find/join_scan
    /*
    {
        // join_find
        execute_statement("create table t5 (c0 int primary key, c1 int)");
        execute_statement("insert into t5 values (3, 30)");

        std::string plan{};
        std::string query = "SELECT t5.c1 FROM t5, t WHERE t5.c0 = clob_length(t.c1)";
        explain_statement(query, plan);
        ASSERT_TRUE(contains(plan, "join_find")) << plan;

        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(30)), result[0]);
    }
    */
}

TEST_F(sql_lob_function_invocation_test, invalid_reference_tag_download) {
    // scenario where function parameter clob gets accidentally invalid tag
    // verify the expression evaluation happens and the tx aborts
    auto called = std::make_shared<bool>(false);
    auto id = 1000UL;  // any value to avoid conflict
    auto client = std::make_shared<data_relay_client>("localhost:52345");

    // dup function duplicates the input CLOB value
    global::scalar_function_repository().add(
        id,
        std::make_shared<scalar_function_info>(
            scalar_function_kind::mock_function_for_testing,
            [called, client](evaluator_context& ectx, sequence_view<data::any> args) -> data::any {
                *called = true;
                auto e = download_lob<lob::clob_reference>(ectx, args[0], *client, 0); // use invalid reference tag
                auto concat = e + e;
                return upload_lob<lob::clob_reference>(ectx, concat, *client);
            },
            1
        )
    );
    decl_ = global::scalar_function_provider()->add({
        id,
        "dup",
        t::clob(),
        {
            t::clob(),
        },
    });
    execute_statement("create table t (c0 int primary key, c1 clob)");
    execute_statement("insert into t values (0, 'ABC'::clob)");
    auto sql = "SELECT dup(c1) FROM t";
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        test_stmt_err(sql, *tx, error_code::value_evaluation_exception);  // TODO should be permission error
        test_stmt_err(sql, *tx, error_code::inactive_transaction_exception);
    }
    EXPECT_TRUE(*called);
}

TEST_F(sql_lob_function_invocation_test, invalid_reference_tag_upload) {
    // scenario where function parameter clob gets accidentally invalid tag
    // verify the expression evaluation happens and the tx aborts
    auto called = std::make_shared<bool>(false);
    auto id = 1000UL;  // any value to avoid conflict
    auto client = std::make_shared<data_relay_client>("localhost:52345");

    // dup function duplicates the input CLOB value
    global::scalar_function_repository().add(
        id,
        std::make_shared<scalar_function_info>(
            scalar_function_kind::mock_function_for_testing,
            [called, client](evaluator_context& ectx, sequence_view<data::any> args) -> data::any {
                *called = true;
                auto ref = args[0].to<lob::clob_reference>();
                EXPECT_TRUE(ref.lob_reference::reference_tag());
                EXPECT_NE(0, ref.lob_reference::reference_tag().value());

                auto e = download_lob<lob::clob_reference>(ectx, args[0], *client, ref.lob_reference::reference_tag().value());
                auto concat = e + e;
                auto uploaded = upload_lob<lob::clob_reference>(ectx, concat, *client);
                auto bad_ref = uploaded.to<lob::clob_reference>().reference_tag(0); // use invalid reference tag
                return data::any{std::in_place_type<lob::clob_reference>, bad_ref};
            },
            1
        )
    );
    decl_ = global::scalar_function_provider()->add({
        id,
        "dup",
        t::clob(),
        {
            t::clob(),
        },
    });
    execute_statement("create table t (c0 int primary key, c1 clob)");
    execute_statement("insert into t values (0, 'ABC'::clob)");
    auto sql = "SELECT dup(c1) FROM t";
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        test_stmt_err(sql, *tx, error_code::permission_error);
        test_stmt_err(sql, *tx, error_code::inactive_transaction_exception);
    }
    EXPECT_TRUE(*called);
}


}  // namespace jogasaki::testing
