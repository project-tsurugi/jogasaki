/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <takatori/type/clob.h>
#include <takatori/type/table.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>
#include <tateyama/api/server/mock/request_response.h>
#include <tateyama/framework/server.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/impl/service.h>
#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/service/bridge.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>
#include <jogasaki/data/mock_any_sequence_stream.h>
#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/function/table_valued_function_info.h>
#include <jogasaki/executor/function/table_valued_function_kind.h>
#include <jogasaki/executor/function/table_valued_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/lob/lob_data_provider.h>
#include <jogasaki/lob/lob_reference.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_utils/create_configuration.h>
#include <jogasaki/test_utils/create_file.h>
#include <jogasaki/test_utils/data_relay_client.h>
#include <jogasaki/test_utils/temporary_folder.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/command_utils.h>
#include <jogasaki/utils/latch.h>
#include <jogasaki/utils/msgbuf_utils.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/utils/tables.h>
#include "jogasaki/proto/sql/common.pb.h"
#include "jogasaki/proto/sql/request.pb.h"
#include "jogasaki/proto/sql/response.pb.h"

#include "../api/api_test_base.h"
#include "service_api_common.h"

namespace jogasaki::api {

using namespace std::chrono_literals;
using namespace std::string_view_literals;
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::utils;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::dto;
using namespace jogasaki::scheduler;
using namespace tateyama::api::server;
namespace sql = jogasaki::proto::sql;
using ValueCase = sql::request::Parameter::ValueCase;
using takatori::util::unsafe_downcast;
using takatori::util::maybe_shared_ptr;

using jogasaki::api::impl::get_impl;
namespace framework = tateyama::framework;

class service_api_lob_function_invocation_test : public service_api_test {

public:
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
        db_->config()->skip_smv_check(true);

        auto c = std::make_shared<tateyama::api::configuration::whole>("");
        service_ = std::make_shared<jogasaki::api::impl::service>(c, db_.get());

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

    std::unique_ptr<framework::server> server_{};
    std::shared_ptr<yugawara::function::declaration> decl_{};
};

TEST_F(service_api_lob_function_invocation_test, lob_types_with_apply) {
    // verify reference tag is set correctly for lob references returned from APPLY operator
    test_statement("create table t (c0 int primary key, c1 clob)");
    test_statement("insert into t values (1, 'ABC'::clob)");

    // Register table-valued function for APPLY
    auto id = 13000UL;
    auto client = std::make_shared<::jogasaki::testing::data_relay_client>("localhost:52345");

    // Register function declaration for SQL compilation
    decl_ = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            id,
            "append",
            std::make_shared<takatori::type::table>(std::initializer_list<takatori::type::table::column_type>{
                {"output", std::make_shared<takatori::type::clob>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{
                std::make_shared<takatori::type::clob>(),
            },
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );

    // Register execution information
    global::table_valued_function_repository().add(
        id,
        std::make_shared<executor::function::table_valued_function_info>(
            executor::function::table_valued_function_kind::builtin,
            [client](
                executor::expr::evaluator_context& ectx,
                takatori::util::sequence_view<data::any> args
            ) -> std::unique_ptr<data::any_sequence_stream> {
                // Download the input CLOB
                auto ref = args[0].to<lob::clob_reference>();
                auto tag = ref.lob_reference::reference_tag();
                auto input_data = [&]() -> std::string {
                    auto provider = ref.provider();
                    std::size_t blob_id = ref.object_id();
                    if (provider == lob::lob_data_provider::datastore && ! tag) {
                        auto* ds = datastore::get_datastore();
                        auto result = ds->get_blob_file(blob_id);
                        if (! result) {
                            takatori::util::throw_exception(std::runtime_error("failed to get blob from datastore"));
                        }
                        return read_file(result.path().string());
                    }
                    auto s = ectx.blob_session()->get_or_create();
                    if (! s) {
                        takatori::util::throw_exception(std::runtime_error(""));
                    }
                    std::uint64_t session_id = s->session_id();
                    std::uint64_t storage_id = (provider == lob::lob_data_provider::datastore) ? 1 : 0;
                    auto t = tag ? tag.value() : s->compute_tag(blob_id);
                    return client->get_blob(session_id, storage_id, blob_id, t);
                }();

                // Create 3 rows with appended data
                std::vector<data::any_sequence> sequences{};
                for (int i = 1; i <= 3; ++i) {
                    auto appended = input_data + std::to_string(i);
                    auto s = ectx.blob_session()->get_or_create();
                    if (! s) {
                        takatori::util::throw_exception(std::runtime_error(""));
                    }
                    std::uint64_t session_id = s->session_id();
                    auto [blob_id, storage_id, tag_val] = client->put_blob(session_id, appended);
                    if (blob_id == 0) {
                        takatori::util::throw_exception(
                            std::runtime_error("put_blob() failed session_id:" + std::to_string(session_id))
                        );
                    }
                    ::jogasaki::lob::clob_reference clob_ref(blob_id, ::jogasaki::lob::lob_data_provider::relay_service_session);
                    clob_ref.reference_tag(tag_val);
                    ::jogasaki::data::any uploaded(std::in_place_type<::jogasaki::lob::clob_reference>, clob_ref);
                    std::vector<::jogasaki::data::any> row;
                    row.emplace_back(uploaded);
                    sequences.emplace_back(::jogasaki::data::any_sequence(std::move(row)));
                }

                return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
            },
            1,
            executor::function::table_valued_function_info::columns_type{
                executor::function::table_valued_function_column{"output"}
            }
        )
    );

    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select t.c0, r.output from t cross apply append(t.c1) as r"
    );

    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                {"c0", common_column::atom_type::int4},
                {"output", common_column::atom_type::clob},
            };
            ASSERT_EQ(exp, cols);

            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(3, v.size());

                auto* ds = datastore::get_datastore();
                for (std::size_t i = 0; i < 3; ++i) {
                    EXPECT_EQ(1, v[i].get_value<std::int32_t>(0)) << "Row " << i;
                    auto clob_ref = v[i].get_value<lob::clob_reference>(1);
                    auto tag = v[i].get_field_value_info(1).blob_reference_tag_;

                    // Verify CLOB reference is in datastore
                    EXPECT_EQ(lob::lob_data_provider::datastore, clob_ref.provider()) << "Row " << i;

                    // Verify reference tag is set
                    EXPECT_NE(0, tag) << "Row " << i << ": reference tag should not be 0";

                    // Retrieve actual CLOB data from datastore
                    auto blob_file_opt = ds->get_blob_file(clob_ref.object_id());
                    ASSERT_TRUE(blob_file_opt) << "Row " << i << ": Failed to get blob from datastore";

                    auto content = read_file(blob_file_opt.path().string());
                    std::string expected = "ABC" + std::to_string(i + 1);
                    EXPECT_EQ(expected, content) << "Row " << i << " content mismatch";

                    // Verify that test_get_lob succeeds with the reference tag
                    test_get_lob(clob_ref.object_id(), tag, blob_file_opt.path().string(), tx_handle.surrogate_id());
                }
            }
        }
        {
            auto [success, error] = decode_result_only(res->body_);
            ASSERT_TRUE(success);
        }
    }
    test_commit(tx_handle);
    test_dispose_prepare(query_handle);
}

}  // namespace jogasaki::api
