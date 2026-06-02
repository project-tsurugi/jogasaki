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

#include <takatori/relation/buffer.h>
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
#include <jogasaki/executor/process/impl/variables_view.h>
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
        variable_table_list variables_list_;
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
            variables_list_{},
            task_ctx_{{}, {}, {}, {}},
            ctx_{&task_ctx_, variables_view{variables_list_, 0}, res, varlen_res}
        {
            variables_list_.emplace_back(block_info);
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
    /**
     * @brief Bundle of runtime objects for a cross-region filter test.
     *
     * The upstream block (index 0) holds the input variables written by take_flat.
     * The filter lives in block 1, downstream of a buffer node, so its
     * variable_table_info has no local entries and must delegate to the parent
     * (block 0) to resolve the input variables.  The variables_view is constructed
     * for block 1, exercising the cross-region read path through region_id.
     */
    struct buffer_filter_executor {
        filter op_;
        variable_table_list variables_list_;
        mock::task_context task_ctx_;
        filter_context ctx_;

        buffer_filter_executor(
            filter op_arg,
            variable_table_info const& upstream_block_info,
            variable_table_info const& filter_block_info,
            std::size_t filter_block_idx,
            std::size_t total_block_count,
            memory::lifo_paged_memory_resource* res,
            memory::lifo_paged_memory_resource* varlen_res,
            request_context* req_ctx
        ) :
            op_{std::move(op_arg)},
            variables_list_{},
            task_ctx_{},
            ctx_{&task_ctx_, variables_view{variables_list_, filter_block_idx}, res, varlen_res}
        {
            variables_list_.reserve(total_block_count);
            variables_list_.emplace_back(upstream_block_info);
            variables_list_.emplace_back(filter_block_info);
            ctx_.task_context().work_context(std::make_unique<impl::work_context>(
                req_ctx, 0, total_block_count, nullptr, nullptr, nullptr, nullptr, false, false
            ));
        }
    };

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
    set_variables(ex.variables_list_[0], in, input_pass.ref());
    ex.op_(ex.ctx_);
    ASSERT_TRUE(called);

    // c1 == c2 + 1: 20 == 22 + 1 → false
    called = false;
    auto input_fail = create_nullable_record<kind::int8, kind::int8, kind::int8>(2, 20, 22);
    set_variables(ex.variables_list_[0], in, input_fail.ref());
    ex.op_(ex.ctx_);
    ASSERT_TRUE(! called);
}

TEST_F(filter_test, cross_region_reads_upstream_variables) {
    // Graph: take_flat(2 cols) → buffer(1) → filter(in[1] == in[0] + 1) → offer
    auto input_pass = create_nullable_record<kind::int8, kind::int8>(10, 11);
    auto [up_ref, in] = add_upstream_record_provider(input_pass.record_meta());
    auto& up = up_ref.get();

    auto& buf_node = emplace_operator<relation::buffer>(1);
    up.output() >> buf_node.input();

    auto& flt = emplace_operator<relation::filter>(std::make_unique<compare>(
        comparison_operator::equal,
        varref(in[1]),
        binary{binary_operator::add, varref(in[0]), constant(1)}
    ));
    auto down = add_downstream_record_verifier({in[0], in[1]});
    buf_node.output_ports()[0] >> flt.input();
    flt.output() >> down.input();

    create_processor_info(nullptr, true);

    auto const take_block_idx   = processor_info_->block_indices().at(&up);
    auto const filter_block_idx = processor_info_->block_indices().at(&flt);
    ASSERT_NE(take_block_idx, filter_block_idx);

    auto const& vti_take   = processor_info_->vars_info_list()[take_block_idx];
    auto const& vti_filter = processor_info_->vars_info_list()[filter_block_idx];

    // Structural: input variables are NOT locally defined in the filter's block
    ASSERT_TRUE(! vti_filter.exists_local(in[0]));
    ASSERT_TRUE(! vti_filter.exists_local(in[1]));
    // Structural: they resolve to the upstream (take) block's region
    ASSERT_EQ(vti_filter.at(in[0]).region().index(), take_block_idx);
    ASSERT_EQ(vti_filter.at(in[1]).region().index(), take_block_idx);

    auto const total_blocks = processor_info_->vars_info_list().size();
    filter op{0, *processor_info_, filter_block_idx, flt.condition(), down.take()};
    auto ex = buffer_filter_executor{
        std::move(op),
        vti_take,
        vti_filter,
        filter_block_idx,
        total_blocks,
        &resource_,
        &varlen_resource_,
        &request_context_
    };

    bool called = false;
    down.set_body([&]() { called = true; });

    // in[1]=11 == in[0]=10 + 1 → filter passes
    set_variables(ex.variables_list_[take_block_idx], in, input_pass.ref());
    ex.op_(ex.ctx_);
    ASSERT_TRUE(called);

    // in[1]=20 != in[0]=10 + 1 → filter blocks
    called = false;
    auto input_fail = create_nullable_record<kind::int8, kind::int8>(10, 20);
    set_variables(ex.variables_list_[take_block_idx], in, input_fail.ref());
    ex.op_(ex.ctx_);
    ASSERT_TRUE(! called);
}

}

