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
#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/relation/filter.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/binary_operator.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/comparison_operator.h>
#include <takatori/scalar/immediate.h>
#include <takatori/type/primitive.h>
#include <takatori/value/primitive.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/impl/ops/filter.h>
#include <jogasaki/executor/process/impl/ops/filter_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/request_context.h>
#include <jogasaki/operator_test_utils.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace jogasaki::mock;
using namespace boost::container::pmr;

namespace type = ::takatori::type;
namespace value = ::takatori::value;
namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;

using binary = takatori::scalar::binary;
using binary_operator = takatori::scalar::binary_operator;
using compare = takatori::scalar::compare;
using comparison_operator = takatori::scalar::comparison_operator;
using immediate = takatori::scalar::immediate;

class filter_test : public test_root, public operator_test_utils {
public:
    immediate constant(int v, type::data&& type = type::int8()) {
        return immediate{value::int8(v), std::move(type)};
    }

    /**
     * @brief Bundle of runtime objects needed to invoke and inspect the filter operator.
     * @details op, variables, task_ctx, and ctx are stored together.
     *     ctx holds references into variables and task_ctx; the struct must not
     *     be move- or copy-constructed.  Always create via make_filter_executor().
     */
    struct filter_executor {
        filter op_;
        variable_table variables_;
        mock::task_context task_ctx_;
        filter_context ctx_;

        filter_executor(
            filter op_arg,
            variable_table_info const& block_info,
            memory::lifo_paged_memory_resource* res,
            memory::lifo_paged_memory_resource* varlen_res,
            request_context* req_ctx
        ) :
            op_{std::move(op_arg)},
            variables_{block_info},
            task_ctx_{{}, {}, {}, {}},
            ctx_{&task_ctx_, variables_, res, varlen_res}
        {
            ctx_.task_context().work_context(std::make_unique<impl::work_context>(
                req_ctx, 0, op_.block_index(), nullptr, nullptr, nullptr, nullptr, false, false
            ));
        }
    };

    /**
     * @brief Wire the process graph, build processor_info, construct the filter operator,
     *     and return a filter_executor.
     *
     * @details Uses C++17 guaranteed copy elision: the returned prvalue is constructed
     *     directly in the caller's variable, so the internal ctx references into
     *     variables and task_ctx remain valid.
     *
     * @param flt  the filter relation node whose condition defines the operator
     * @param up   the upstream take_flat node
     * @param down downstream verifier sink (take() is called here)
     * @return newly constructed filter_executor
     */
    filter_executor make_filter_executor(
        relation::filter& flt,
        relation::step::take_flat& up,
        record_verifier_sink& down
    ) {
        up.output() >> flt.input();
        flt.output() >> down.input();
        create_processor_info(nullptr, true);
        filter op{0, *processor_info_, 0, flt.condition(), down.take()};
        auto const idx = op.block_index();
        return filter_executor{std::move(op), processor_info_->vars_info_list()[idx],
            &resource_, &varlen_resource_, &request_context_};
    }
};

TEST_F(filter_test, simple) {
    // c1 == c2 + 1
    auto input_pass = create_nullable_record<kind::int8, kind::int8, kind::int8>(1, 11, 10);
    auto [up, in] = add_upstream_record_provider(input_pass.record_meta());

    auto& flt = emplace_operator<relation::filter>(std::make_unique<compare>(
        comparison_operator::equal,
        varref(in[1]),
        binary{binary_operator::add, varref(in[2]), constant(1)}
    ));

    bool called = false;
    auto down = add_downstream_record_verifier({in[0], in[1], in[2]});
    auto ex = make_filter_executor(flt, up, down);
    down.set_body([&]() { called = true; });

    // c1 == c2 + 1: 11 == 10 + 1 → true
    set_variables(ex.variables_, in, input_pass.ref());
    ex.op_(ex.ctx_);
    ASSERT_TRUE(called);

    // c1 == c2 + 1: 20 == 22 + 1 → false
    called = false;
    auto input_fail = create_nullable_record<kind::int8, kind::int8, kind::int8>(2, 20, 22);
    set_variables(ex.variables_, in, input_fail.ref());
    ex.op_(ex.ctx_);
    ASSERT_TRUE(! called);
}

}

