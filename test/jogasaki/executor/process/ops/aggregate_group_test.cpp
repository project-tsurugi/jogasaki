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
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <map>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/descriptor/variable.h>
#include <takatori/relation/step/aggregate.h>
#include <takatori/type/primitive.h>
#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/aggregate/declaration.h>

#include <jogasaki/data/value_store.h>
#include <jogasaki/executor/function/builtin_functions.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/ops/aggregate_group.h>
#include <jogasaki/executor/process/impl/ops/aggregate_group_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/operator_test_utils.h>
#include <jogasaki/test_root.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

namespace type = ::takatori::type;
namespace relation = ::takatori::relation;

using kind = meta::field_type_kind;

namespace t = takatori::type;

class aggregate_group_test : public test_root, public operator_test_utils {
public:
    /**
     * @brief Build and register all built-in aggregate functions.
     * @return shared_ptr to the configured provider
     */
    static std::shared_ptr<yugawara::aggregate::configurable_provider> make_functions() {
        auto p = std::make_shared<yugawara::aggregate::configurable_provider>();
        function::add_builtin_aggregate_functions(*p, global::aggregate_function_repository());
        return p;
    }

    std::shared_ptr<yugawara::aggregate::configurable_provider> functions_{make_functions()};  //NOLINT

    /**
     * @brief Find an aggregate function declaration by name and first argument type.
     * @param name     function name (e.g. "count$distinct")
     * @param arg_type expected takatori type of the first parameter
     * @return shared_ptr to the matching declaration, or nullptr if not found
     */
    auto find_aggregate_function(
        std::string_view name,
        ::takatori::type::data const& arg_type
    ) {
        std::shared_ptr<yugawara::aggregate::declaration const> result{};
        functions_->each(name, 1, [&](std::shared_ptr<yugawara::aggregate::declaration const> const& decl) {
            if (! result && ! decl->parameter_types().empty() && decl->parameter_types()[0] == arg_type) {
                result = decl;
            }
        });
        return bindings_(result);
    }

    /**
     * @brief Bundle of runtime objects needed to invoke and inspect the aggregate_group operator.
     * @details ctx_ holds references into variables_ and task_ctx_; the struct must not
     *     be move- or copy-constructed.  Always create via make_agg_executor().
     */
    struct aggregate_group_executor {
        aggregate_group op_;
        variable_table variables_;
        mock::task_context task_ctx_;
        aggregate_group_context ctx_;

        aggregate_group_executor(
            aggregate_group op_arg,
            variable_table_info const& block_info,
            memory::lifo_paged_memory_resource* res,
            memory::lifo_paged_memory_resource* varlen_res,
            std::vector<data::value_store> stores,
            std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> store_resources,
            std::vector<std::vector<std::reference_wrapper<data::value_store>>> function_arg_stores,
            std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> nulls_resources
        ) :
            op_{std::move(op_arg)},
            variables_{block_info},
            task_ctx_{{}, {}, {}, {}},
            ctx_{&task_ctx_, variables_, res, varlen_res,
                std::move(stores), std::move(store_resources),
                std::move(function_arg_stores), std::move(nulls_resources)}
        {}
    };

    /**
     * @brief Wire the process graph, build processor_info, and construct the aggregate_group executor.
     *
     * @details Discovers per-argument value_store types automatically from the compiled variable
     *     mappings.  Uses C++17 guaranteed copy elision for the return value so that ctx_'s
     *     internal references into variables_ and task_ctx_ remain valid.
     *
     * @param agg  the aggregate relation node
     * @param up   the upstream take_group node
     * @param down downstream verifier sink (take() is called here)
     * @return newly constructed aggregate_group_executor
     */
    aggregate_group_executor make_agg_executor(
        relation::step::aggregate& agg,
        relation::step::take_group& up,
        record_verifier_sink& down
    ) {
        up.output() >> agg.input();
        agg.output() >> down.input();
        create_processor_info(nullptr, true);

        // Collect unique argument variables (deduplicated, in first-appearance order),
        // mirroring the internal logic of aggregate_group::create_arguments().
        std::vector<takatori::descriptor::variable> unique_args{};
        std::unordered_map<takatori::descriptor::variable, std::size_t> arg_index_map{};
        for (auto const& col : agg.columns()) {
            for (auto const& arg : col.arguments()) {
                if (arg_index_map.count(arg) == 0) {
                    arg_index_map[arg] = unique_args.size();
                    unique_args.push_back(arg);
                }
            }
        }

        // Allocate one value_store per unique argument.
        std::vector<data::value_store> stores{};
        std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> store_resources{};
        std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> nulls_resources{};
        stores.reserve(unique_args.size());
        store_resources.reserve(unique_args.size());
        nulls_resources.reserve(unique_args.size());
        for (auto const& arg : unique_args) {
            auto const type = utils::type_for(*compiler_info_, arg);
            auto& res = store_resources.emplace_back(
                std::make_unique<memory::lifo_paged_memory_resource>(&pool_)
            );
            auto& nulls_res = nulls_resources.emplace_back(
                std::make_unique<memory::lifo_paged_memory_resource>(&pool_)
            );
            stores.emplace_back(type, res.get(), &varlen_resource_, nulls_res.get());
        }

        // Map each aggregate column's arguments to the corresponding stores.
        std::vector<std::vector<std::reference_wrapper<data::value_store>>> function_arg_stores{};
        function_arg_stores.reserve(agg.columns().size());
        for (auto const& col : agg.columns()) {
            auto& store_refs = function_arg_stores.emplace_back();
            store_refs.reserve(col.arguments().size());
            for (auto const& arg : col.arguments()) {
                store_refs.emplace_back(stores[arg_index_map.at(arg)]);
            }
        }

        auto op = aggregate_group{0, *processor_info_, 0, agg.columns(), down.take()};
        auto const idx = op.block_index();
        return aggregate_group_executor{
            std::move(op),
            processor_info_->vars_info_list()[idx],
            &resource_,
            &varlen_resource_,
            std::move(stores),
            std::move(store_resources),
            std::move(function_arg_stores),
            std::move(nulls_resources)
        };
    }
};

TEST_F(aggregate_group_test, simple) {
    // Upstream: c0 (group key, int8), c1 (value, int8), c2 (value, int8)
    auto [up, in] = add_upstream_group_provider(
        create_meta<kind::int8>(true),
        create_meta<kind::int8, kind::int8>(true)
    );
    auto const& c0 = in.key(0);
    auto const& c1 = in.value(0);
    auto const& c2 = in.value(1);

    // Output variables for aggregate results (count$distinct)
    auto rc1 = bindings_.stream_variable();
    auto rc2 = bindings_.stream_variable();

    auto fn = find_aggregate_function("count$distinct", t::int8{});

    auto& agg = emplace_operator<relation::step::aggregate>(
        std::vector<relation::step::aggregate::column>{
            {fn, c1, rc1},
            {fn, c2, rc2},
        }
    );

    auto down = add_downstream_record_verifier({c0, rc1, rc2});
    auto ex = make_agg_executor(agg, up, down);

    std::size_t called_cnt = 0;
    down.set_body([&]() {
        auto result = get_variables(ex.variables_, {rc1, rc2});
        switch (called_cnt) {
            case 0:
                ASSERT_EQ((create_nullable_record<kind::int8, kind::int8>(2, 3)), result);
                break;
            case 1:
                ASSERT_EQ((create_nullable_record<kind::int8, kind::int8>(3, 2)), result);
                break;
            default:
                FAIL() << "Unexpected number of calls to verifier: " << called_cnt;
        }
        ++called_cnt;
    });

    // Test data: key → sequence of (c1, c2) pairs for that group.
    // group 0 (c0=0): c1 values {1,2,2} → count$distinct=2; c2 values {1,2,3} → count$distinct=3
    // group 1 (c0=1): c1 values {1,2,3} → count$distinct=3; c2 values {1,2,2} → count$distinct=2
    std::map<std::int64_t, std::vector<std::pair<std::int64_t, std::int64_t>>> const groups{
        {0, {{1, 1}, {2, 2}, {2, 3}}},
        {1, {{1, 1}, {2, 2}, {3, 2}}},
    };

    for (auto const& [key, rows] : groups) {
        auto key_rec = create_nullable_record<kind::int8>(key);
        set_variables(ex.variables_, in.key_input(), key_rec.ref());
        for (std::size_t i = 0; i < rows.size(); ++i) {
            auto val = create_nullable_record<kind::int8, kind::int8>(rows[i].first, rows[i].second);
            set_variables(ex.variables_, in.value_input(), val.ref());
            auto kind = (i + 1 == rows.size()) ? member_kind::last_member : member_kind::normal;
            ex.op_(ex.ctx_, kind);
        }
    }
    ASSERT_EQ(groups.size(), called_cnt);
}

TEST_F(aggregate_group_test, basic) {
    // Upstream: c0 (group key, int8), c1 (value, int8)
    auto [up, in] = add_upstream_group_provider(
        create_meta<kind::int8>(true),
        create_meta<kind::int8>(true)
    );
    auto const& c0 = in.key(0);
    auto const& c1 = in.value(0);

    auto rc1 = bindings_.stream_variable();
    auto fn = find_aggregate_function("count$distinct", t::int8{});

    auto& agg = emplace_operator<relation::step::aggregate>(
        std::vector<relation::step::aggregate::column>{
            {fn, c1, rc1},
        }
    );

    auto down = add_downstream_record_verifier({c0, rc1});
    auto ex = make_agg_executor(agg, up, down);

    std::size_t called_cnt = 0;
    down.set_body([&]() {
        auto result = get_variables(ex.variables_, {rc1});
        switch (called_cnt) {
            case 0:
                ASSERT_EQ((create_nullable_record<kind::int8>(std::int64_t{2})), result);
                break;
            default:
                FAIL() << "Unexpected number of calls to verifier: " << called_cnt;
        }
        ++called_cnt;
    });

    // Test data: key → sequence of c1 values for that group.
    // group 0 (c0=0): c1 values {1,2,2} → count$distinct=2
    std::map<std::int64_t, std::vector<std::int64_t>> const groups{
        {0, {1, 2, 2}},
    };

    for (auto const& [key, rows] : groups) {
        auto key_rec = create_nullable_record<kind::int8>(key);
        set_variables(ex.variables_, in.key_input(), key_rec.ref());
        for (std::size_t i = 0; i < rows.size(); ++i) {
            auto val = create_nullable_record<kind::int8>(rows[i]);
            set_variables(ex.variables_, in.value_input(), val.ref());
            auto mk = (i + 1 == rows.size()) ? member_kind::last_member : member_kind::normal;
            ex.op_(ex.ctx_, mk);
        }
    }
    ASSERT_EQ(groups.size(), called_cnt);
}

TEST_F(aggregate_group_test, two_columns_sharing_arg) {
    // regression scenario for #1463
    // Upstream: c0 (group key, int8), c1 (value, int8)
    // Both agg columns use the same argument c1.
    auto [up, in] = add_upstream_group_provider(
        create_meta<kind::int8>(true),
        create_meta<kind::int8>(true)
    );
    auto const& c0 = in.key(0);
    auto const& c1 = in.value(0);

    auto rc1 = bindings_.stream_variable();
    auto rc2 = bindings_.stream_variable();
    auto fn = find_aggregate_function("count$distinct", t::int8{});

    auto& agg = emplace_operator<relation::step::aggregate>(
        std::vector<relation::step::aggregate::column>{
            {fn, c1, rc1},
            {fn, c1, rc2},
        }
    );

    auto down = add_downstream_record_verifier({c0, rc1, rc2});
    auto ex = make_agg_executor(agg, up, down);

    std::size_t called_cnt = 0;
    down.set_body([&]() {
        auto result = get_variables(ex.variables_, {rc1, rc2});
        switch (called_cnt) {
            case 0:
                // group 0: c1={1,2,2} → count$distinct=2 for both columns
                ASSERT_EQ((create_nullable_record<kind::int8, kind::int8>(2, 2)), result);
                break;
            case 1:
                // group 1: c1={1,2,3} → count$distinct=3 for both columns
                ASSERT_EQ((create_nullable_record<kind::int8, kind::int8>(3, 3)), result);
                break;
            default:
                FAIL() << "Unexpected number of calls to verifier: " << called_cnt;
        }
        ++called_cnt;
    });

    // Test data: key → sequence of c1 values for that group.
    // group 0 (c0=0): c1 values {1,2,2} → count$distinct=2
    // group 1 (c0=1): c1 values {1,2,3} → count$distinct=3
    std::map<std::int64_t, std::vector<std::int64_t>> const groups{
        {0, {1, 2, 2}},
        {1, {1, 2, 3}},
    };

    for (auto const& [key, rows] : groups) {
        auto key_rec = create_nullable_record<kind::int8>(key);
        set_variables(ex.variables_, in.key_input(), key_rec.ref());
        for (std::size_t i = 0; i < rows.size(); ++i) {
            auto val = create_nullable_record<kind::int8>(rows[i]);
            set_variables(ex.variables_, in.value_input(), val.ref());
            auto mk = (i + 1 == rows.size()) ? member_kind::last_member : member_kind::normal;
            ex.op_(ex.ctx_, mk);
        }
    }
    ASSERT_EQ(groups.size(), called_cnt);
}

}

