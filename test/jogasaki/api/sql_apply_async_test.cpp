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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/type/primitive.h>
#include <takatori/type/table.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>

#include <jogasaki/configuration.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/function/table_valued_function_info.h>
#include <jogasaki/executor/function/table_valued_function_kind.h>
#include <jogasaki/executor/function/table_valued_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/custom_any_sequence_stream.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::mock;
using takatori::util::sequence_view;
using executor::expr::evaluator_context;
using namespace executor::function;
namespace t = takatori::type;

using kind = meta::field_type_kind;

/**
 * @brief test for APPLY operator with asynchronous (not_ready) TVF stream behaviour.
 * @details the apply operator must correctly yield and resume when a TVF stream returns
 *          not_ready, and must produce the correct result records for all rows.
 */
class sql_apply_async_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        global::table_valued_function_repository().clear();
        for (auto const& decl : registered_decls_) {
            global::regular_function_provider()->remove(*decl);
        }
        registered_decls_.clear();
        db_teardown();
    }

    std::vector<std::shared_ptr<yugawara::function::declaration>> registered_decls_{};
};

/**
 * @brief verify that the apply operator correctly yields and resumes when try_next returns not_ready.
 * @details the UDTF returns 3 rows but try_next does not immediately deliver each record:
 *   - row 1 (1, 10):   returned on the 1st try_next call (immediately).
 *   - row 2 (2, 20):   1 not_ready, then returned on the 2nd try for this row.
 *   - row 3 (3, 30):   2 not_ready, then returned on the 3rd try for this row.
 *   - end_of_stream:   3 not_ready, then returned on the 4th try.
 *
 * total try_next call count: 1 + 2 + 3 + 4 = 10.
 *
 * the test verifies both the exact call count and the correctness of the result records.
 */
TEST_F(sql_apply_async_test, three_rows_with_not_ready) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");

    // shared counter: counts every try_next invocation across the single stream instance.
    auto try_next_count = std::make_shared<std::atomic_int>(0);

    constexpr std::size_t tvf_id = 12200;

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id,
            "tvf_async_3rows",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::int4>()},
                {"c2", std::make_shared<t::int8>()},
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
            [try_next_count](
                evaluator_context& /* ctx */,
                sequence_view<data::any> /* args */
            ) -> std::unique_ptr<data::any_sequence_stream> {
                // the stream delivers rows according to the prescribed not_ready schedule:
                //   call 0  → ok   (row 1: c1=1, c2=10)
                //   call 1  → not_ready
                //   call 2  → ok   (row 2: c1=2, c2=20)
                //   calls 3,4 → not_ready
                //   call 5  → ok   (row 3: c1=3, c2=30)
                //   calls 6,7,8 → not_ready
                //   call 9  → end_of_stream
                // total: 10 calls.
                return std::make_unique<custom_any_sequence_stream>(
                    [try_next_count](data::any_sequence& seq)
                        -> data::any_sequence_stream::status_type {
                        auto cnt = try_next_count->fetch_add(1);
                        switch (cnt) {
                            case 0:
                                seq = data::any_sequence{data::any_sequence::storage_type{
                                    data::any{std::in_place_type<std::int32_t>, std::int32_t{1}},
                                    data::any{std::in_place_type<std::int64_t>, std::int64_t{10}}
                                }};
                                return data::any_sequence_stream::status_type::ok;
                            case 1:
                                return data::any_sequence_stream::status_type::not_ready;
                            case 2:
                                seq = data::any_sequence{data::any_sequence::storage_type{
                                    data::any{std::in_place_type<std::int32_t>, std::int32_t{2}},
                                    data::any{std::in_place_type<std::int64_t>, std::int64_t{20}}
                                }};
                                return data::any_sequence_stream::status_type::ok;
                            case 3:
                            case 4:
                                return data::any_sequence_stream::status_type::not_ready;
                            case 5:
                                seq = data::any_sequence{data::any_sequence::storage_type{
                                    data::any{std::in_place_type<std::int32_t>, std::int32_t{3}},
                                    data::any{std::in_place_type<std::int64_t>, std::int64_t{30}}
                                }};
                                return data::any_sequence_stream::status_type::ok;
                            case 6:
                            case 7:
                            case 8:
                                return data::any_sequence_stream::status_type::not_ready;
                            default:
                                return data::any_sequence_stream::status_type::end_of_stream;
                        }
                    }
                );
            },
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"},
                table_valued_function_column{"c2"}
            }
        )
    );

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT R.c1, R.c2 FROM T CROSS APPLY tvf_async_3rows(T.C0) AS R(c1, c2)",
        result
    );

    // verify total try_next call count: 1 (row1) + 2 (row2) + 3 (row3) + 4 (end) = 10
    EXPECT_EQ(10, try_next_count->load());

    // verify result records
    ASSERT_EQ(3, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8>(1, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8>(2, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8>(3, 30)), result[2]);
}

/**
 * @brief verify that nested double-APPLY correctly resumes after each level yields.
 * @details two APPLY operators are chained:
 *   FROM T CROSS APPLY tvf_2rows_outer(T.C0) AS R1 CROSS APPLY tvf_2rows_inner(R1.c1) AS R2
 *
 * both TVFs share the same per-stream call schedule:
 *   - call 0 → ok  (row 1: c1=1, c2=10)   : returned immediately on the 1st try
 *   - call 1 → not_ready                   : 1 not_ready before row 2
 *   - call 2 → ok  (row 2: c1=2, c2=20)   : returned on the 2nd try
 *   - call 3 → end_of_stream
 *
 * with T having one row (C0=1) the execution proceeds as follows:
 *   outer A.call_0 → ok  (R1=(1,10)) → inner B processes R1:
 *       inner B.call_0 → ok  (R2=(1,10)) → output (1,1,1)
 *       inner B.call_1 → not_ready → inner APPLY yields (propagates to outer APPLY)
 *       inner B.call_2 → ok  (R2=(2,20)) → output (1,1,2)
 *       inner B.call_3 → end_of_stream → inner APPLY done for R1
 *   outer A.call_1 → not_ready → outer APPLY yields
 *   outer A.call_2 → ok  (R1=(2,20)) → inner C processes R1:
 *       inner C.call_0 → ok  (R2=(1,10)) → output (1,2,1)
 *       inner C.call_1 → not_ready → inner APPLY yields
 *       inner C.call_2 → ok  (R2=(2,20)) → output (1,2,2)
 *       inner C.call_3 → end_of_stream → inner APPLY done for R2
 *   outer A.call_3 → end_of_stream → outer APPLY done
 *
 * expected try_next call counts:
 *   - outer: 4  (1 stream instance × 4 calls)
 *   - inner: 8  (2 stream instances × 4 calls each)
 *
 * expected result records: (T.C0, R1.c1, R2.c1) = (1,1,1), (1,1,2), (1,2,1), (1,2,2)
 */
TEST_F(sql_apply_async_test, double_apply_with_not_ready) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");

    auto outer_try_next_count = std::make_shared<std::atomic_int>(0);
    auto inner_try_next_count = std::make_shared<std::atomic_int>(0);

    // helper that builds a TVF factory using the given global counter.
    // each stream instance maintains its own per-call index (n) independently.
    auto make_tvf_factory = [](std::shared_ptr<std::atomic_int> total_count) {
        return [total_count](
            evaluator_context& /* ctx */,
            sequence_view<data::any> /* args */
        ) -> std::unique_ptr<data::any_sequence_stream> {
            return std::make_unique<custom_any_sequence_stream>(
                // n is captured by value and starts at 0 for each new stream instance
                [total_count, n = 0](data::any_sequence& seq) mutable
                    -> data::any_sequence_stream::status_type {
                    total_count->fetch_add(1);
                    auto cnt = n++;
                    switch (cnt) {
                        case 0:
                            seq = data::any_sequence{data::any_sequence::storage_type{
                                data::any{std::in_place_type<std::int32_t>, std::int32_t{1}},
                                data::any{std::in_place_type<std::int64_t>, std::int64_t{10}}
                            }};
                            return data::any_sequence_stream::status_type::ok;
                        case 1:
                            return data::any_sequence_stream::status_type::not_ready;
                        case 2:
                            seq = data::any_sequence{data::any_sequence::storage_type{
                                data::any{std::in_place_type<std::int32_t>, std::int32_t{2}},
                                data::any{std::in_place_type<std::int64_t>, std::int64_t{20}}
                            }};
                            return data::any_sequence_stream::status_type::ok;
                        default:
                            return data::any_sequence_stream::status_type::end_of_stream;
                    }
                }
            );
        };
    };

    constexpr std::size_t tvf_id_outer = 12201;
    constexpr std::size_t tvf_id_inner = 12202;

    auto decl_outer = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id_outer,
            "tvf_2rows_outer",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::int4>()},
                {"c2", std::make_shared<t::int8>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{
                std::make_shared<t::int4>(),
            },
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    registered_decls_.emplace_back(decl_outer);

    global::table_valued_function_repository().add(
        tvf_id_outer,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            make_tvf_factory(outer_try_next_count),
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"},
                table_valued_function_column{"c2"}
            }
        )
    );

    auto decl_inner = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id_inner,
            "tvf_2rows_inner",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::int4>()},
                {"c2", std::make_shared<t::int8>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{
                std::make_shared<t::int4>(),
            },
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    registered_decls_.emplace_back(decl_inner);

    global::table_valued_function_repository().add(
        tvf_id_inner,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            make_tvf_factory(inner_try_next_count),
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"},
                table_valued_function_column{"c2"}
            }
        )
    );

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R1.c1, R2.c1 "
        "FROM T "
        "CROSS APPLY tvf_2rows_outer(T.C0) AS R1(c1, c2) "
        "CROSS APPLY tvf_2rows_inner(R1.c1) AS R2(c1, c2)",
        result
    );

    // outer: 1 stream instance × 4 calls (ok, not_ready, ok, end) = 4
    EXPECT_EQ(4, outer_try_next_count->load());
    // inner: 2 stream instances × 4 calls each = 8
    EXPECT_EQ(8, inner_try_next_count->load());

    // 4 result rows: (T.C0=1, R1.c1, R2.c1) = {1,2} x {1,2}
    ASSERT_EQ(4, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 1, 1)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 1, 2)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 2, 1)), result[2]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 2, 2)), result[3]);
}

}  // namespace jogasaki::testing
