/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/type/lob.h>
#include <takatori/type/table.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>
#include <tateyama/framework/server.h>

#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/api/service/bridge.h>
#include <jogasaki/configuration.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence_stream.h>
#include <jogasaki/data/mock_any_sequence_stream.h>
#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/function/table_valued_function_info.h>
#include <jogasaki/executor/function/table_valued_function_kind.h>
#include <jogasaki/executor/function/table_valued_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/lob/lob_reference.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/create_configuration.h>
#include <jogasaki/test_utils/create_file.h>
#include <jogasaki/test_utils/data_relay_client.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"
#include "lob_test_helper.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::mock;
using namespace tateyama;
using takatori::util::sequence_view;
using executor::expr::evaluator_context;
using namespace jogasaki::executor::function;
namespace t = takatori::type;

using kind = meta::field_type_kind;

/**
 * @brief test for APPLY operator with BLOB/CLOB
 */
class sql_apply_blob_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        temporary_.prepare();

        grpc_port_ = 52345 + (std::hash<std::thread::id>{}(std::this_thread::get_id()) % 1000);
        auto conf = test_utils::create_configuration(
            path() + "/log_location",
            path() + "/session_store",
            grpc_port_
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
        global::table_valued_function_repository().clear();
        if (decl_) {
            global::regular_function_provider()->remove(*decl_);
        }
    }

    [[nodiscard]] std::string path() const {
        return temporary_.path();
    }

    std::size_t grpc_port_{};
    std::unique_ptr<framework::server> server_{};
    std::shared_ptr<yugawara::function::declaration> decl_{}; // needed here to remove even if test fails
};

TEST_F(sql_apply_blob_test, apply_with_clob) {
    // Register table-valued function "append" that takes a CLOB parameter
    // and returns a table with a single CLOB column named "output"
    // The function returns 3 rows, each with the input appended with "1", "2", "3"

    auto id = 13000UL;  // any value to avoid conflict
    auto client = std::make_shared<data_relay_client>("localhost:" + std::to_string(grpc_port_));

    // Register function declaration for SQL compilation
    decl_ = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            id,
            "append",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"output", std::make_shared<t::clob>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{
                std::make_shared<t::clob>(),  // input parameter
            },
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );

    // Register execution information
    global::table_valued_function_repository().add(
        id,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            [client](
                evaluator_context& ectx,
                sequence_view<data::any> args
            ) -> std::unique_ptr<data::any_sequence_stream> {
                // Download the input CLOB - handle both datastore and session storage
                auto ref = args[0].to<lob::clob_reference>();
                auto input_data = download_lob<lob::clob_reference>(
                    ectx, args[0], *client, ref.lob_reference::reference_tag().value()
                );

                // Create 3 rows with appended data
                std::vector<data::any_sequence> sequences{};
                for (int i = 1; i <= 3; ++i) {
                    auto appended = input_data + std::to_string(i);
                    auto uploaded = upload_lob<lob::clob_reference>(ectx, appended, *client);

                    sequences.emplace_back(std::vector<data::any>{uploaded});
                }

                return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
            },
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"output"}
            }
        )
    );

    // Create test table and insert data
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 CLOB)");
    execute_statement("INSERT INTO T VALUES (1, 'ABC'::clob)");

    // Execute query with APPLY
    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query(
        "SELECT T.C0, R.output FROM T CROSS APPLY append(T.C1) AS R",
        *tx,
        result
    );

    // Verify we got 3 rows
    ASSERT_EQ(3, result.size());

    // Verify each row's data by retrieving from datastore
    // Note: Currently, the CLOB references returned from APPLY are in session storage,
    // not in datastore. This test expects them to be in datastore (which is the correct
    // behavior), so it will fail until the implementation is fixed.
    auto* ds = datastore::get_datastore();
    for (std::size_t i = 0; i < 3; ++i) {
        // Check C0 value
        EXPECT_EQ(1, result[i].get_value<std::int32_t>(0));

        // Get CLOB reference
        auto clob_ref = result[i].get_value<lob::clob_reference>(1);
        // reference tag is generated in the data_channel_writer, while test suite uses result_store_writer, which
        // does not generate reference tag. So we cannot check reference tag here.

        // SQL results should always have CLOB references in datastore
        EXPECT_EQ(lob::lob_data_provider::datastore, clob_ref.provider()) << "Row " << i;

        // Retrieve actual CLOB data from datastore using get_blob_file
        auto blob_file_opt = ds->get_blob_file(clob_ref.object_id());
        ASSERT_TRUE(blob_file_opt) << "Row " << i << ": Failed to get blob from datastore";

        auto content = read_file(blob_file_opt.path().string());
        std::string expected = "ABC" + std::to_string(i + 1);
        EXPECT_EQ(expected, content) << "Row " << i << " content mismatch";

        LOG(INFO) << "Row " << i << ": Successfully retrieved from datastore, content='" << content << "'";
    }

    EXPECT_EQ(status::ok, tx->commit());
}

}  // namespace jogasaki::testing
