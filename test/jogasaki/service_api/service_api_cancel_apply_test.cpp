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
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/type/primitive.h>
#include <takatori/type/table.h>
#include <takatori/util/sequence_view.h>
#include <tateyama/api/server/mock/request_response.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/service.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/function/table_valued_function_info.h>
#include <jogasaki/executor/function/table_valued_function_kind.h>
#include <jogasaki/executor/function/table_valued_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/request_cancel_config.h>
#include <jogasaki/test_utils/mock_any_sequence_stream.h>
#include <jogasaki/utils/command_utils.h>
#include "jogasaki/proto/sql/request.pb.h"
#include "jogasaki/proto/sql/response.pb.h"

#include "service_api_common.h"

namespace jogasaki::api {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::executor;
using namespace jogasaki::executor::function;
using namespace tateyama::api::server;
namespace t = takatori::type;
using takatori::util::sequence_view;

std::string serialize(sql::request::Request& r);
void deserialize(std::string_view s, sql::response::Response& res);

// ---------------------------------------------------------------------------
// Stream helpers for cancel testing
// ---------------------------------------------------------------------------

/**
 * @brief an any_sequence_stream whose try_next behaviour is provided by a caller-supplied lambda.
 * @details the lambda is invoked on every try_next call and controls what is returned.
 *          this makes it easy to inject cancel signals, not_ready transitions, or error
 *          conditions at precisely the desired point in a test scenario.
 *
 *          example — arm cancel on the first call and return not_ready:
 * @code
 *   auto stream = std::make_unique<custom_any_sequence_stream>(
 *       [res](data::any_sequence&) -> data::any_sequence_stream::status_type {
 *           res->cancel();
 *           return data::any_sequence_stream::status_type::not_ready;
 *       }
 *   );
 * @endcode
 */
class custom_any_sequence_stream : public data::any_sequence_stream {
public:
    using handler_type = std::function<status_type(data::any_sequence&)>;

    /**
     * @brief constructs the stream with a custom try_next handler.
     * @param handler called on every try_next invocation.
     */
    explicit custom_any_sequence_stream(handler_type handler) :
        handler_(std::move(handler))
    {}

    [[nodiscard]] status_type try_next(data::any_sequence& sequence) override {
        return handler_(sequence);
    }

    [[nodiscard]] status_type next(
        data::any_sequence& sequence,
        std::optional<std::chrono::milliseconds> /* timeout */) override {
        return try_next(sequence);
    }

    void close() override {}

private:
    handler_type handler_{};
};

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

/**
 * @brief test fixture for apply operator cancel behaviour.
 * @details extends service_api_test with UDTF registration/cleanup support.
 */
class service_api_cancel_apply_test : public service_api_test {
public:
    void TearDown() override {
        service_api_test::TearDown();
        global::table_valued_function_repository().clear();
        for (auto const& decl : registered_decls_) {
            global::regular_function_provider()->remove(*decl);
        }
        registered_decls_.clear();
    }

    /**
     * @brief run a cancel-query request for the given sql, expecting OPERATION_CANCELED.
     * @details the caller is responsible for pre-arming res->cancel() when needed.
     */
    void run_cancel_query(
        std::string_view sql,
        api::transaction_handle tx_handle,
        std::shared_ptr<tateyama::api::server::mock::test_response> res
    ) {
        auto s = encode_execute_query(tx_handle, sql);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        EXPECT_TRUE(res->all_released());
        EXPECT_EQ(::tateyama::proto::diagnostics::Code::OPERATION_CANCELED, res->error_.code());
    }

    std::vector<std::shared_ptr<yugawara::function::declaration>> registered_decls_{};
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/**
 * @brief verify that a query containing APPLY is immediately cancelled before the UDTF is called.
 * @details when the apply operator is active and cancel is pre-armed on the response,
 *          the operator's entry-point cancel check should fire before calling the UDTF function body.
 *          the UDTF_call_count must remain 0.
 */
TEST_F(service_api_cancel_apply_test, cancel_apply_immediate) {
    enable_request_cancel(request_cancel_kind::apply);

    // atomic counter to detect whether the UDTF body was ever invoked
    auto udtf_call_count = std::make_shared<std::atomic_int>(0);

    constexpr std::size_t tvf_id = 14100;

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id,
            "tvf_cancel_immediate",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::int4>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{
                std::make_shared<t::int4>(),
            },
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    registered_decls_.emplace_back(decl);

    global::table_valued_function_repository().add(
        tvf_id,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            [udtf_call_count](
                executor::expr::evaluator_context& /* ectx */,
                sequence_view<data::any> /* args */
            ) -> std::unique_ptr<data::any_sequence_stream> {
                udtf_call_count->fetch_add(1);
                return std::make_unique<testing::mock_any_sequence_stream>();
            },
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );

    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (1)");

    api::transaction_handle tx_handle{};
    test_begin(tx_handle);

    // arm cancel before the request is submitted: the apply entry-point cancel check fires immediately
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    res->cancel();
    run_cancel_query("select t.c0, r.c1 from t cross apply tvf_cancel_immediate(t.c0) as r",
        tx_handle, res);

    // UDTF must not have been called since cancel was detected before function invocation
    EXPECT_EQ(0, udtf_call_count->load());

    test_commit(tx_handle, false, error_code::inactive_transaction_exception);
    test_get_error_info(tx_handle, false, error_code::none);
}

/**
 * @brief verify that cancel is detected after the UDTF is called but before the first try_next.
 * @details the UDTF body itself arms the cancel flag on the response and then returns a simple
 *          stream.  When the apply operator enters the streaming loop the first cancel check
 *          (before any try_next call) must fire, aborting the request without ever calling try_next.
 */
TEST_F(service_api_cancel_apply_test, cancel_apply_on_udtf_call) {
    enable_request_cancel(request_cancel_kind::apply);

    auto try_next_count = std::make_shared<std::atomic_int>(0);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    constexpr std::size_t tvf_id = 14101;

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id,
            "tvf_cancel_on_call",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::int4>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{
                std::make_shared<t::int4>(),
            },
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    registered_decls_.emplace_back(decl);

    global::table_valued_function_repository().add(
        tvf_id,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            [res, try_next_count](
                executor::expr::evaluator_context& /* ectx */,
                sequence_view<data::any> /* args */
            ) -> std::unique_ptr<data::any_sequence_stream> {
                // arm cancel: the apply operator will detect it at the try_next-loop cancel check
                res->cancel();

                // return a stream that counts every try_next invocation
                return std::make_unique<custom_any_sequence_stream>(
                    [try_next_count](data::any_sequence& /* seq */)
                        -> data::any_sequence_stream::status_type {
                        try_next_count->fetch_add(1);
                        return data::any_sequence_stream::status_type::end_of_stream;
                    }
                );
            },
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );

    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (1)");

    api::transaction_handle tx_handle{};
    test_begin(tx_handle);

    run_cancel_query("select t.c0, r.c1 from t cross apply tvf_cancel_on_call(t.c0) as r",
        tx_handle, res);

    // try_next was never called — cancel was detected in the streaming-loop cancel check
    // before the first try_next call
    EXPECT_EQ(0, try_next_count->load());

    test_commit(tx_handle, false, error_code::inactive_transaction_exception);
    test_get_error_info(tx_handle, false, error_code::none);
}

/**
 * @brief verify that cancel is detected after the first try_next returns not_ready.
 * @details the UDTF returns a stream whose first try_next call arms cancel and returns not_ready,
 *          causing the apply operator to yield.  When the operator resumes, the entry-point cancel
 *          check fires before re-entering the streaming loop, so the second try_next is never called.
 */
TEST_F(service_api_cancel_apply_test, cancel_apply_on_first_try_next) {
    enable_request_cancel(request_cancel_kind::apply);

    auto try_next_count = std::make_shared<std::atomic_int>(0);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    constexpr std::size_t tvf_id = 14102;

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id,
            "tvf_cancel_on_try_next",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::int4>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{
                std::make_shared<t::int4>(),
            },
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    registered_decls_.emplace_back(decl);

    global::table_valued_function_repository().add(
        tvf_id,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            [res, try_next_count](
                executor::expr::evaluator_context& /* ectx */,
                sequence_view<data::any> /* args */
            ) -> std::unique_ptr<data::any_sequence_stream> {
                // return a stream whose first try_next arms cancel and returns not_ready
                return std::make_unique<custom_any_sequence_stream>(
                    [res, try_next_count](data::any_sequence& /* seq */)
                        -> data::any_sequence_stream::status_type {
                        auto cnt = try_next_count->fetch_add(1);
                        if (cnt == 0) {
                            // first call: arm cancel and yield the operator
                            res->cancel();
                            return data::any_sequence_stream::status_type::not_ready;
                        }
                        // should not reach here; cancel must have been detected before second call
                        return data::any_sequence_stream::status_type::end_of_stream;
                    }
                );
            },
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );

    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (1)");

    api::transaction_handle tx_handle{};
    test_begin(tx_handle);

    run_cancel_query("select t.c0, r.c1 from t cross apply tvf_cancel_on_try_next(t.c0) as r",
        tx_handle, res);

    // first try_next was called (count == 1); second try_next must NOT have been reached
    EXPECT_EQ(1, try_next_count->load());

    test_commit(tx_handle, false, error_code::inactive_transaction_exception);
    test_get_error_info(tx_handle, false, error_code::none);
}

}  // namespace jogasaki::api
