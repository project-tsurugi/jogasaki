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
#include <string_view>
#include <vector>

#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/relation/project.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/binary_operator.h>
#include <takatori/scalar/expression.h>
#include <takatori/scalar/immediate.h>
#include <takatori/type/character.h>
#include <takatori/type/data.h>
#include <takatori/type/primitive.h>
#include <takatori/type/varying.h>
#include <takatori/util/rvalue_reference_wrapper.h>
#include <takatori/value/character.h>
#include <takatori/value/primitive.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/process/impl/ops/project.h>
#include <jogasaki/executor/process/impl/ops/project_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/request_context.h>
#include <jogasaki/executor/process/mock/task_context.h>
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
using immediate = takatori::scalar::immediate;

class project_test : public test_root, public operator_test_utils {
public:
    immediate constant(int v, type::data&& type = type::int8()) {
        return immediate{value::int8(v), std::move(type)};
    }
    immediate constant_text(std::string_view v, type::data&& type = type::character(type::varying, 64)) {
        return immediate{value::character(v), std::move(type)};
    }

    /**
     * @brief Bundle of runtime objects needed to invoke and inspect the project operator.
     * @details op, variables, task_ctx, and ctx are stored together.
     *     ctx holds references into variables and task_ctx; the struct must not
     *     be move- or copy-constructed.  Always create via make_project_executor().
     */
    struct project_executor {
        project op_;
        variable_table variables_;
        mock::task_context task_ctx_;
        project_context ctx_;

        project_executor(
            project op,
            variable_table_info const& block_info,
            memory::lifo_paged_memory_resource* res,
            memory::lifo_paged_memory_resource* varlen_res,
            request_context* req_ctx
        ) :
            op_{std::move(op)},
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
     * @brief Wire the process graph, build processor_info, construct the project operator,
     *     and return a project_executor.
     *
     * @details Uses C++17 guaranteed copy elision: the returned prvalue is constructed
     *     directly in the caller's variable, so the internal ctx references into
     *     variables and task_ctx remain valid.
     *
     * @param prj  the project relation node whose columns define the operator
     * @param up   the upstream take_flat node (pass the reference-wrapper from
     *             add_upstream_record_provider directly; implicit conversion applies)
     * @param down downstream verifier sink (take() is called here)
     * @return newly constructed project_executor
     */
    project_executor make_project_executor(
        relation::project& prj,
        relation::step::take_flat& up,
        record_verifier_sink& down
    ) {
        up.output() >> prj.input();
        prj.output() >> down.input();
        create_processor_info(nullptr, true);
        project op{0, *processor_info_, 0, prj.columns(), down.take()};
        auto const idx = op.block_index();
        return project_executor{std::move(op), processor_info_->vars_info_list()[idx],
            &resource_, &varlen_resource_, &request_context_};
    }
};

TEST_F(project_test, simple) {
    auto input = create_nullable_record<kind::int8, kind::int8, kind::int8>(1, 11, 10);
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    auto out = create_output_variables(2);

    using column = relation::project::column;
    auto& prj = emplace_operator<relation::project>(
        std::initializer_list<rvalue_reference_wrapper<column>>{
            column{out[0], constant(100)},
            column{out[1], binary{binary_operator::add, varref(in[1]),
                binary{binary_operator::add, varref(in[2]), constant(1)}}},
        });

    bool called = false;
    auto down = add_downstream_record_verifier({in[0], in[1], in[2], out[0], out[1]});
    auto ex = make_project_executor(prj, up, down);
    down.set_body([&]() {
        EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(100, 22)),
            get_variables(ex.variables_, {out[0], out[1]}));
        called = true;
    });
    set_variables(ex.variables_, in, input.ref());
    ex.op_(ex.ctx_);
    ASSERT_TRUE(called);
}

TEST_F(project_test, text) {
    auto input = create_nullable_record<kind::character, kind::character, kind::character>(
        text{&resource_, "A23456789012345678901234567890"},
        text{&resource_, "B23456789012345678901234567890"},
        text{&resource_, "C23456789012345678901234567890"});
    auto [up, in] = add_upstream_record_provider(input.record_meta());
    auto out = create_output_variables(1);

    using column = relation::project::column;
    auto& prj = emplace_operator<relation::project>(
        std::initializer_list<rvalue_reference_wrapper<column>>{
            column{out[0], binary{binary_operator::concat, varref(in[1]),
                binary{binary_operator::concat, varref(in[2]),
                    constant_text("Z23456789012345678901234567890")}}},
        });

    bool called = false;
    auto down = add_downstream_record_verifier({in[0], in[1], in[2], out[0]});
    auto ex = make_project_executor(prj, up, down);
    down.set_body([&]() {
        EXPECT_EQ(
            (create_nullable_record<kind::character>(text{
                &resource_,
                "B23456789012345678901234567890"
                "C23456789012345678901234567890"
                "Z23456789012345678901234567890"
            })),
            get_variables(ex.variables_, {out[0]})
        );
        called = true;
    });
    set_variables(ex.variables_, in, input.ref());
    ex.op_(ex.ctx_);
    ASSERT_TRUE(called);
}

}
