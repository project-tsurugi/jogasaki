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
#include <jogasaki/executor/process/impl/expression_evaluator.h>

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <boost/dynamic_bitset.hpp>

#include <jogasaki/executor/partitioner.h>
#include <jogasaki/executor/comparator.h>

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/binding/factory.h>
#include <yugawara/runtime_feature.h>
#include <yugawara/compiler.h>
#include <yugawara/compiler_options.h>

#include <mizugaki/translator/shakujo_translator.h>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/util/string_builder.h>
#include <takatori/util/downcast.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/buffer.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/project.h>
#include <takatori/statement/write.h>
#include <takatori/statement/execute.h>
#include <takatori/scalar/immediate.h>
#include <takatori/plan/process.h>
#include <takatori/plan/forward.h>
#include <takatori/serializer/json_printer.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/binary_operator.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/comparison_operator.h>

#include <takatori/util/enum_tag.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/test_root.h>

#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/impl/ops/operator_builder.h>
#include <jogasaki/executor/process/impl/block_scope.h>

namespace jogasaki::executor::process::impl {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;
using namespace yugawara::binding;

using namespace ::mizugaki::translator;
using namespace ::mizugaki;

using namespace testing;

using code = shakujo_translator_code;
using result_kind = shakujo_translator::result_type::kind_type;

namespace type = ::takatori::type;
namespace value = ::takatori::value;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace statement = ::takatori::statement;


using take = relation::step::take_flat;
using offer = relation::step::offer;
using buffer = relation::buffer;

using rgraph = ::takatori::relation::graph_type;

class expression_evaluator_test : public test_root {
public:

    yugawara::analyzer::variable_mapping& variables() noexcept {
        return *variables_;
    }

    yugawara::analyzer::expression_mapping& expressions() noexcept {
        return *expressions_;
    }

    std::shared_ptr<yugawara::analyzer::variable_mapping> variables_ = std::make_shared<yugawara::analyzer::variable_mapping>();
    std::shared_ptr<yugawara::analyzer::expression_mapping> expressions_ = std::make_shared<yugawara::analyzer::expression_mapping>();
};

using binary = takatori::scalar::binary;
using binary_operator = takatori::scalar::binary_operator;
using compare = takatori::scalar::compare;
using comparison_operator = takatori::scalar::comparison_operator;
using unary = takatori::scalar::unary;
using unary_operator = takatori::scalar::unary_operator;
using immediate = takatori::scalar::immediate;
using compiled_info = yugawara::compiled_info;

TEST_F(expression_evaluator_test, add_int8) {
    factory f;
    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");

    binary expr {
        binary_operator::add,
        varref(c1),
        varref(c2)
    };
    expressions().bind(expr, t::int8 {});
    expressions().bind(expr.left(), t::int8 {});
    expressions().bind(expr.right(), t::int8 {});

    compiled_info c_info{expressions_, variables_};
    expression_evaluator ev{expr, c_info};

    std::unique_ptr<variable_value_map> value_map{};
    maybe_shared_ptr<meta::record_meta> meta = std::make_shared<meta::record_meta>(
        std::vector<meta::field_type>{
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int8>),
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{std::string("00")}
    );

    std::unordered_map<variable, std::size_t> m{
        {c1, 0},
        {c2, 1},
    };

    block_scope_info info{m, meta};
    block_scope scope{info};

    auto&& ref = scope.store().ref();
    ref.set_value<std::int64_t>(meta->value_offset(0), 10);
    ref.set_value<std::int64_t>(meta->value_offset(1), 20);

    auto result = ev(scope).to<std::int64_t>();
    ASSERT_EQ(30, result);
}

TEST_F(expression_evaluator_test, add_int4) {
    factory f;
    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");

    binary expr {
        binary_operator::add,
        varref(c1),
        varref(c2)
    };
    expressions().bind(expr, t::int4 {});
    expressions().bind(expr.left(), t::int4 {});
    expressions().bind(expr.right(), t::int4 {});

    compiled_info c_info{expressions_, variables_};
    expression_evaluator ev{expr, c_info};

    maybe_shared_ptr<meta::record_meta> meta = std::make_shared<meta::record_meta>(
        std::vector<meta::field_type>{
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int4>),
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int4>),
        },
        boost::dynamic_bitset<std::uint64_t>{std::string("00")}
    );

    std::unordered_map<variable, std::size_t> m{
        {c1, 0},
        {c2, 1},
    };

    block_scope_info info{m, meta};
    block_scope scope{info};

    auto&& ref = scope.store().ref();
    ref.set_value<std::int32_t>(meta->value_offset(0), 10);
    ref.set_value<std::int32_t>(meta->value_offset(1), 20);

    auto result = ev(scope).to<std::int32_t>();
    ASSERT_EQ(30, result);
}

TEST_F(expression_evaluator_test, add_float4) {
    factory f;
    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");

    binary expr {
        binary_operator::add,
        varref(c1),
        varref(c2)
    };
    expressions().bind(expr, t::float4 {});
    expressions().bind(expr.left(), t::float4 {});
    expressions().bind(expr.right(), t::float4 {});

    compiled_info c_info{expressions_, variables_};
    expression_evaluator ev{expr, c_info};

    std::unique_ptr<variable_value_map> value_map{};
    maybe_shared_ptr<meta::record_meta> meta = std::make_shared<meta::record_meta>(
        std::vector<meta::field_type>{
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::float4>),
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::float4>),
        },
        boost::dynamic_bitset<std::uint64_t>{std::string("00")}
    );

    std::unordered_map<variable, std::size_t> m{
        {c1, 0},
        {c2, 1},
    };

    block_scope_info info{m, meta};
    block_scope scope{info};

    auto&& ref = scope.store().ref();
    ref.set_value<float>(meta->value_offset(0), 10);
    ref.set_value<float>(meta->value_offset(1), 20);

    auto result = ev(scope).to<float>();
    ASSERT_EQ(30, result);
}

TEST_F(expression_evaluator_test, add_double) {
    factory f;
    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");

    binary expr {
        binary_operator::add,
        varref(c1),
        varref(c2)
    };
    expressions().bind(expr, t::float8 {});
    expressions().bind(expr.left(), t::float8 {});
    expressions().bind(expr.right(), t::float8 {});

    compiled_info c_info{expressions_, variables_};
    expression_evaluator ev{expr, c_info};

    std::unique_ptr<variable_value_map> value_map{};
    maybe_shared_ptr<meta::record_meta> meta = std::make_shared<meta::record_meta>(
        std::vector<meta::field_type>{
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::float8>),
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::float8>),
        },
        boost::dynamic_bitset<std::uint64_t>{std::string("00")}
    );

    std::unordered_map<variable, std::size_t> m{
        {c1, 0},
        {c2, 1},
    };

    block_scope_info info{m, meta};
    block_scope scope{info};

    auto&& ref = scope.store().ref();
    ref.set_value<double>(meta->value_offset(0), 10);
    ref.set_value<double>(meta->value_offset(1), 20);

    auto result = ev(scope).to<double>();
    ASSERT_EQ(30, result);
}

inline immediate constant(int v, type::data&& type = type::int8()) {
    return immediate { value::int8(v), std::move(type) };
}

inline immediate constant_bool(bool v, type::data&& type = type::boolean()) {
    return immediate { value::boolean(v), std::move(type) };
}

TEST_F(expression_evaluator_test, binary_expression) {
    factory f;

    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");

    binary expr {
        binary_operator::subtract,
        varref(c1),
        binary {
            binary_operator::add,
            varref(c2),
            constant(30)
        }
    };
    expressions().bind(expr, t::int8 {});
    expressions().bind(expr.left(), t::int8 {});
    expressions().bind(expr.right(), t::int8 {});

    auto& r = static_cast<binary&>(expr.right());
    expressions().bind(r.left(), t::int8 {});
    expressions().bind(r.right(), t::int8 {});

    compiled_info c_info{
        expressions_,
        variables_
    };
    expression_evaluator ev{expr, c_info};

    std::unique_ptr<variable_value_map> value_map{};
    maybe_shared_ptr<meta::record_meta> meta = std::make_shared<meta::record_meta>(
        std::vector<meta::field_type>{
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int8>),
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{std::string("00")}
    );

    std::unordered_map<variable, std::size_t> m{
        {c1, 0},
        {c2, 1},
    };

    block_scope_info info{m, meta};
    block_scope scope{info};

    auto&& ref = scope.store().ref();
    ref.set_value<std::int64_t>(meta->value_offset(0), 10);
    ref.set_value<std::int64_t>(meta->value_offset(1), 20);

    auto result = ev(scope).to<std::int64_t>();
    ASSERT_EQ(-40, result);
}

TEST_F(expression_evaluator_test, unary_expression) {
    unary expr {
        unary_operator::sign_inversion,
        unary {
            unary_operator::plus,
            constant(30)
        }
    };
    expressions().bind(expr, t::int8 {});
    expressions().bind(expr.operand(), t::int8 {});

    auto& o = static_cast<unary&>(expr.operand());
    expressions().bind(o.operand(), t::int8 {});

    compiled_info c_info{
        expressions_,
        variables_
    };
    expression_evaluator ev{expr, c_info};

    block_scope scope{};
    auto result = ev(scope).to<std::int64_t>();
    ASSERT_EQ(-30, result);
}

TEST_F(expression_evaluator_test, conditional_not) {
    unary expr {
        unary_operator::conditional_not,
        constant_bool(false)
    };
    expressions().bind(expr, t::boolean {});
    expressions().bind(expr.operand(), t::boolean {});

    compiled_info c_info{ expressions_, variables_ };
    expression_evaluator ev{expr, c_info};

    block_scope scope{};
    ASSERT_TRUE(ev(scope).to<bool>());
}


TEST_F(expression_evaluator_test, compare_int4) {
    factory f;

    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");

    compare expr {
        comparison_operator::less,
        varref(c1),
        varref(c2),
    };
    expressions().bind(expr, t::int4 {});
    expressions().bind(expr.left(), t::int4 {});
    expressions().bind(expr.right(), t::int4 {});

    compiled_info c_info{ expressions_, variables_ };
    expression_evaluator ev{expr, c_info};

    std::unique_ptr<variable_value_map> value_map{};
    maybe_shared_ptr<meta::record_meta> meta = std::make_shared<meta::record_meta>(
        std::vector<meta::field_type>{
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int4>),
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int4>),
        },
        boost::dynamic_bitset<std::uint64_t>{std::string("00")}
    );

    std::unordered_map<variable, std::size_t> m{
        {c1, 0},
        {c2, 1},
    };

    block_scope_info info{m, meta};
    block_scope scope{info};

    auto&& ref = scope.store().ref();
    ref.set_value<std::int32_t>(meta->value_offset(0), 1);
    ref.set_value<std::int32_t>(meta->value_offset(1), 2);
    ASSERT_TRUE(ev(scope).to<bool>());
    ref.set_value<std::int32_t>(meta->value_offset(0), 1);
    ref.set_value<std::int32_t>(meta->value_offset(1), 1);
    ASSERT_FALSE(ev(scope).to<bool>());

    expr.operator_kind(comparison_operator::less_equal);
    ref.set_value<std::int32_t>(meta->value_offset(0), 1);
    ref.set_value<std::int32_t>(meta->value_offset(1), 2);
    ASSERT_TRUE(ev(scope).to<bool>());
    ref.set_value<std::int32_t>(meta->value_offset(0), 1);
    ref.set_value<std::int32_t>(meta->value_offset(1), 1);
    ASSERT_TRUE(ev(scope).to<bool>());
    ref.set_value<std::int32_t>(meta->value_offset(0), 2);
    ref.set_value<std::int32_t>(meta->value_offset(1), 1);
    ASSERT_FALSE(ev(scope).to<bool>());

    expr.operator_kind(comparison_operator::greater);
    ref.set_value<std::int32_t>(meta->value_offset(0), 2);
    ref.set_value<std::int32_t>(meta->value_offset(1), 1);
    ASSERT_TRUE(ev(scope).to<bool>());
    ref.set_value<std::int32_t>(meta->value_offset(0), 1);
    ref.set_value<std::int32_t>(meta->value_offset(1), 1);
    ASSERT_FALSE(ev(scope).to<bool>());

    expr.operator_kind(comparison_operator::greater_equal);
    ref.set_value<std::int32_t>(meta->value_offset(0), 2);
    ref.set_value<std::int32_t>(meta->value_offset(1), 1);
    ASSERT_TRUE(ev(scope).to<bool>());
    ref.set_value<std::int32_t>(meta->value_offset(0), 1);
    ref.set_value<std::int32_t>(meta->value_offset(1), 1);
    ASSERT_TRUE(ev(scope).to<bool>());
    ref.set_value<std::int32_t>(meta->value_offset(0), 1);
    ref.set_value<std::int32_t>(meta->value_offset(1), 2);
    ASSERT_FALSE(ev(scope).to<bool>());

    expr.operator_kind(comparison_operator::equal);
    ref.set_value<std::int32_t>(meta->value_offset(0), 1);
    ref.set_value<std::int32_t>(meta->value_offset(1), 1);
    ASSERT_TRUE(ev(scope).to<bool>());
    expr.operator_kind(comparison_operator::not_equal);
    ASSERT_FALSE(ev(scope).to<bool>());
    expr.operator_kind(comparison_operator::equal);
    ref.set_value<std::int32_t>(meta->value_offset(0), 1);
    ref.set_value<std::int32_t>(meta->value_offset(1), 2);
    ASSERT_FALSE(ev(scope).to<bool>());
    expr.operator_kind(comparison_operator::not_equal);
    ASSERT_TRUE(ev(scope).to<bool>());
}

TEST_F(expression_evaluator_test, conditional_and) {
    factory f;

    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");

    binary expr {
        binary_operator::conditional_and,
        varref(c1),
        varref(c2),
    };

    expressions().bind(expr, t::boolean {});
    expressions().bind(expr.left(), t::boolean {});
    expressions().bind(expr.right(), t::boolean {});

    compiled_info c_info{ expressions_, variables_ };
    expression_evaluator ev{expr, c_info};

    std::unique_ptr<variable_value_map> value_map{};
    maybe_shared_ptr<meta::record_meta> meta = std::make_shared<meta::record_meta>(
        std::vector<meta::field_type>{
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::boolean>),
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::boolean>),
        },
        boost::dynamic_bitset<std::uint64_t>{std::string("00")}
    );

    std::unordered_map<variable, std::size_t> m{
        {c1, 0},
        {c2, 1},
    };
    block_scope_info info{m, meta};
    block_scope scope{info};

    auto&& ref = scope.store().ref();
    ref.set_value<std::int8_t>(meta->value_offset(0), 1);
    ref.set_value<std::int8_t>(meta->value_offset(1), 1);
    ASSERT_TRUE(ev(scope).to<bool>());
    ref.set_value<std::int8_t>(meta->value_offset(0), 1);
    ref.set_value<std::int8_t>(meta->value_offset(1), 0);
    ASSERT_FALSE(ev(scope).to<bool>());
    ref.set_value<std::int8_t>(meta->value_offset(0), 0);
    ref.set_value<std::int8_t>(meta->value_offset(1), 0);
    ASSERT_FALSE(ev(scope).to<bool>());

    expr.operator_kind(binary_operator::conditional_or);
    ref.set_value<std::int8_t>(meta->value_offset(0), 1);
    ref.set_value<std::int8_t>(meta->value_offset(1), 1);
    ASSERT_TRUE(ev(scope).to<bool>());
    ref.set_value<std::int8_t>(meta->value_offset(0), 1);
    ref.set_value<std::int8_t>(meta->value_offset(1), 0);
    ASSERT_TRUE(ev(scope).to<bool>());
    ref.set_value<std::int8_t>(meta->value_offset(0), 0);
    ref.set_value<std::int8_t>(meta->value_offset(1), 0);
    ASSERT_FALSE(ev(scope).to<bool>());
}

}

