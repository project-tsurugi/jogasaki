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
#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <string>
#include <string_view>
#include <type_traits>
#include <boost/container/container_fwd.hpp>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/graph/graph.h>
#include <takatori/graph/port.h>
#include <takatori/plan/forward.h>
#include <takatori/plan/graph.h>
#include <takatori/plan/group.h>
#include <takatori/plan/process.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/step/join.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_cogroup.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/unary.h>
#include <takatori/scalar/unary_operator.h>
#include <takatori/type/primitive.h>
#include <takatori/util/exception.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/reference_iterator.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/sequence.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/process/impl/ops/join.h>
#include <jogasaki/executor/process/impl/ops/join_context.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/take_cogroup.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/mock/group_reader.h>
#include <jogasaki/executor/process/mock/iterable_group_store.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/utils/iterator_pair.h>

#include "verifier.h"

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

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;

namespace storage = yugawara::storage;

class join_test : public test_root {
public:
};

using kind = field_type_kind;
using group_reader = mock::basic_group_reader;
using group_type = group_reader::group_type;
using keys_type = group_type::key_type;
using values_type = group_type::value_type;

TEST_F(join_test, simple) {

    binding::factory bindings;

    auto&& g0c0 = bindings.exchange_column("g0c0");
    auto&& g0c1 = bindings.exchange_column("g0c1");
    auto&& g0c2 = bindings.exchange_column("g0c2");
    ::takatori::plan::group g0{
        {
            g0c0,
            g0c1,
            g0c2,
        },
        {
            g0c0,
            g0c1,
        },
    };
    auto&& g1c0 = bindings.exchange_column("g1c0");
    auto&& g1c1 = bindings.exchange_column("g1c1");
    auto&& g1c2 = bindings.exchange_column("g1c2");
    ::takatori::plan::group g1{
        {
            g1c0,
            g1c1,
            g1c2,
        },
        {
            g1c0,
            g1c1,
        },
    };
    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});
    auto g0v0 = bindings.stream_variable("g0v0");
    auto g0v1 = bindings.stream_variable("g0v1");
    auto g0v2 = bindings.stream_variable("g0v2");
    auto g1v0 = bindings.stream_variable("g1v0");
    auto g1v1 = bindings.stream_variable("g1v1");
    auto g1v2 = bindings.stream_variable("g1v2");

    auto& r0 = p0.operators().insert(relation::step::take_cogroup {
        {
            bindings(g0),
            {
                { g0c0, g0v0 },
                { g0c1, g0v1 },
                { g0c2, g0v2 },
            },
        },
        {
            bindings(g1),
            {
                { g1c0, g1v0 },
                { g1c1, g1v1 },
                { g1c2, g1v2 },
            },
        }
    });

    auto&& r1 = p0.operators().insert(relation::step::join {
        relation::step::join::operator_kind_type::inner
    });
    r0.output() >> r1.input();
    ::takatori::plan::forward f1 {
        bindings.exchange_column("f1g0v0"),
        bindings.exchange_column("f1g0v1"),
        bindings.exchange_column("f1g0v2"),
        bindings.exchange_column("f1g1v0"),
        bindings.exchange_column("f1g1v1"),
        bindings.exchange_column("f1g1v2"),
    };
    // without offer, the columns are not used and block variables become empty
    auto&& r2 = p0.operators().insert(relation::step::offer {
        bindings(f1),
        {
            { g0v0, f1.columns()[0] },
            { g0v1, f1.columns()[1] },
            { g0v2, f1.columns()[2] },
            { g1v0, f1.columns()[3] },
            { g1v1, f1.columns()[4] },
            { g1v2, f1.columns()[5] },
        }
    });
    r1.output() >> r2.input(); // connection required by takatori
    auto vmap = std::make_shared<yugawara::analyzer::variable_mapping>();
    vmap->bind(g0c0, t::int8{});
    vmap->bind(g0c1, t::int4{});
    vmap->bind(g0c2, t::int8{});
    vmap->bind(g1c0, t::int8{});
    vmap->bind(g1c1, t::int4{});
    vmap->bind(g1c2, t::int8{});
    vmap->bind(g0v0, t::int8{});
    vmap->bind(g0v1, t::int4{});
    vmap->bind(g0v2, t::int8{});
    vmap->bind(g1v0, t::int8{});
    vmap->bind(g1v1, t::int4{});
    vmap->bind(g1v2, t::int8{});
    yugawara::compiled_info c_info{{}, vmap};

    processor_info p_info{p0.operators(), c_info};

    meta::variable_order order0{
        variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>,
        g0.columns(),
        g0.group_keys()
    };
    meta::variable_order order1{
        variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>,
        g1.columns(),
        g1.group_keys()
    };
    auto input_meta = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int4>),
            field_type(field_enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{3}.flip()
    );

    auto tgt = jogasaki::mock::create_nullable_record<kind::int8, kind::int4, kind::int8, kind::int8, kind::int4, kind::int8>();
    auto key = jogasaki::mock::create_nullable_record<kind::int8, kind::int4>();
    auto value = jogasaki::mock::create_nullable_record<kind::int8>();
    auto key_meta = key.record_meta();
    auto value_meta = value.record_meta();
    auto g_meta = group_meta{key_meta, value_meta};
    auto tmeta = tgt.record_meta();
    variable_table_info block_info{
        {
            { g0v0, { tmeta->value_offset(0), tmeta->nullity_offset(0), 0} },
            { g0v1, { tmeta->value_offset(1), tmeta->nullity_offset(1), 1} },
            { g0v2, { tmeta->value_offset(2), tmeta->nullity_offset(2), 2} },
            { g1v0, { tmeta->value_offset(3), tmeta->nullity_offset(3), 3} },
            { g1v1, { tmeta->value_offset(4), tmeta->nullity_offset(4), 4} },
            { g1v2, { tmeta->value_offset(5), tmeta->nullity_offset(5), 5} },
        },
        tmeta,
    };
    variable_table variables{block_info};

    std::vector<ops::group_element> groups{};
    groups.emplace_back(
        order0,
        maybe_shared_ptr(&g_meta),
        r0.groups()[0].columns(),
        0,
        block_info
    );
    groups.emplace_back(
        order1,
        maybe_shared_ptr(&g_meta),
        r0.groups()[1].columns(),
        1,
        block_info
    );

    using join_kind = relation::step::join::operator_kind_type;

    using iterator = mock::iterable_group_store::iterator;
    auto d = std::make_unique<verifier>();
    auto downstream = d.get();

    join<iterator> j{
        0,
        p_info,
        0,
        join_kind::inner,
        takatori::util::optional_ptr<takatori::scalar::expression const>{},  //TODO add expression
        std::move(d)
    };

    mock::task_context task_ctx{
        {},
        {},
        {},
        {},
    };

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    memory::lifo_paged_memory_resource varlen_resource{&pool};
    join_context ctx(
        &task_ctx,
        variables,
        &resource,
        &varlen_resource
    );

    std::vector<jogasaki::mock::basic_record> result{};

    downstream->body([&]() {
        result.emplace_back(jogasaki::mock::basic_record(variables.store().ref(), tmeta));
    });

    mock::iterable_group_store ge1{
        jogasaki::mock::create_nullable_record<kind::int8, kind::int4>(1,10),
        {
            jogasaki::mock::create_nullable_record<kind::int8>(100),
            jogasaki::mock::create_nullable_record<kind::int8>(101),
        }
    };
    mock::iterable_group_store ge2{
        jogasaki::mock::create_nullable_record<kind::int8, kind::int4>(1,10),
        {
            jogasaki::mock::create_nullable_record<kind::int8>(200),
            jogasaki::mock::create_nullable_record<kind::int8>(201),
            jogasaki::mock::create_nullable_record<kind::int8>(202),
        }
    };

    std::vector<group_field> fields[2];
    std::size_t tgt_field = 0;
    for(std::size_t loop = 0; loop < 2; ++loop) { // left then right
        for(std::size_t i=0, n=key.record_meta()->field_count() ; i < n; ++i) {
            auto& meta = key.record_meta();
            fields[loop].emplace_back(
                meta->at(i),
                meta->value_offset(i),
                tgt.record_meta()->value_offset(tgt_field),
                meta->nullity_offset(i),
                tgt.record_meta()->nullity_offset(tgt_field),
                true,
                true
            );
            ++tgt_field;
        }
        for(std::size_t i=0, n=value.record_meta()->field_count() ; i < n; ++i) {
            auto& meta = value.record_meta();
            fields[loop].emplace_back(
                meta->at(i),
                meta->value_offset(i),
                tgt.record_meta()->value_offset(tgt_field),
                meta->nullity_offset(i),
                tgt.record_meta()->nullity_offset(tgt_field),
                true,
                false
            );
            ++tgt_field;
        }
    }
    using iterator_pair = utils::iterator_pair<iterator>;
    std::vector<ops::group<iterator>> mygroups{
        group{
            iterator_pair{
                ge1.begin(),
                ge1.end()
            },
            fields[0],
            ge1.key().ref(),
            ge1.values()[0].record_meta()->record_size()
        },
        group{
            iterator_pair{
                ge2.begin(),
                ge2.end()
            },
            fields[1],
            ge2.key().ref(),
            ge2.values()[0].record_meta()->record_size()
        }
    };
    cogroup<iterator> mycgrp{
        mygroups
    };
    j(ctx, mycgrp);

    ASSERT_EQ(6, result.size());
    std::vector<jogasaki::mock::basic_record> exp{
        jogasaki::mock::create_nullable_record<kind::int8, kind::int4, kind::int8, kind::int8, kind::int4, kind::int8>(1,10,100,1,10,200),
        jogasaki::mock::create_nullable_record<kind::int8, kind::int4, kind::int8, kind::int8, kind::int4, kind::int8>(1,10,100,1,10,201),
        jogasaki::mock::create_nullable_record<kind::int8, kind::int4, kind::int8, kind::int8, kind::int4, kind::int8>(1,10,100,1,10,202),
        jogasaki::mock::create_nullable_record<kind::int8, kind::int4, kind::int8, kind::int8, kind::int4, kind::int8>(1,10,101,1,10,200),
        jogasaki::mock::create_nullable_record<kind::int8, kind::int4, kind::int8, kind::int8, kind::int4, kind::int8>(1,10,101,1,10,201),
        jogasaki::mock::create_nullable_record<kind::int8, kind::int4, kind::int8, kind::int8, kind::int4, kind::int8>(1,10,101,1,10,202),
    };
    std::sort(exp.begin(), exp.end());
    std::sort(result.begin(), result.end());
    ASSERT_EQ(exp, result);
    ctx.release();
}

TEST_F(join_test, left_join_with_condition) {
    // issue 583 - left join with condition (is null) generated wrong result
    binding::factory bindings;

    auto&& g0c0 = bindings.exchange_column("g0c0");
    auto&& g0c2 = bindings.exchange_column("g0c2");
    ::takatori::plan::group g0{
        {
            g0c0,
            g0c2,
        },
        {
            g0c0,
        },
    };
    auto&& g1c0 = bindings.exchange_column("g1c0");
    auto&& g1c2 = bindings.exchange_column("g1c2");
    ::takatori::plan::group g1{
        {
            g1c0,
            g1c2,
        },
        {
            g1c0,
        },
    };
    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});
    auto g0v0 = bindings.stream_variable("g0v0");
    auto g0v2 = bindings.stream_variable("g0v2");
    auto g1v0 = bindings.stream_variable("g1v0");
    auto g1v2 = bindings.stream_variable("g1v2");

    auto& r0 = p0.operators().insert(relation::step::take_cogroup {
        {
            bindings(g0),
            {
                { g0c0, g0v0 },
                { g0c2, g0v2 },
            },
        },
        {
            bindings(g1),
            {
                { g1c0, g1v0 },
                { g1c2, g1v2 },
            },
        }
    });

    auto exp0 = varref(g1v0);
    auto&& r1 = p0.operators().insert(relation::step::join{
        relation::step::join::operator_kind_type::left_outer,
        std::make_unique<scalar::unary>(scalar::unary_operator::is_null, std::move(exp0))
    });
    r0.output() >> r1.input();
    ::takatori::plan::forward f1 {
        bindings.exchange_column("f1g0v0"),
        bindings.exchange_column("f1g0v2"),
        bindings.exchange_column("f1g1v0"),
        bindings.exchange_column("f1g1v2"),
    };
    // without offer, the columns are not used and block variables become empty
    auto&& r2 = p0.operators().insert(relation::step::offer {
        bindings(f1),
        {
            { g0v0, f1.columns()[0] },
            { g0v2, f1.columns()[1] },
            { g1v0, f1.columns()[2] },
            { g1v2, f1.columns()[3] },
        }
    });
    r1.output() >> r2.input(); // connection required by takatori
    auto vmap = std::make_shared<yugawara::analyzer::variable_mapping>();
    vmap->bind(g0c0, t::int8{});
    vmap->bind(g0c2, t::int8{});
    vmap->bind(g1c0, t::int8{});
    vmap->bind(g1c2, t::int8{});
    vmap->bind(g0v0, t::int8{});
    vmap->bind(g0v2, t::int8{});
    vmap->bind(g1v0, t::int8{});
    vmap->bind(g1v2, t::int8{});

    auto emap = std::make_shared<yugawara::analyzer::expression_mapping>();
    auto& u = static_cast<scalar::unary&>(*r1.condition());
    emap->bind(u.operand(), t::int8{});
    emap->bind(*r1.condition(), t::boolean{});
    yugawara::compiled_info c_info{emap, vmap};

    processor_info p_info{p0.operators(), c_info};

    meta::variable_order order0{
        variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>,
        g0.columns(),
        g0.group_keys()
    };
    meta::variable_order order1{
        variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>,
        g1.columns(),
        g1.group_keys()
    };
    auto input_meta = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{2}.flip()
    );

    auto tgt = jogasaki::mock::create_nullable_record<kind::int8, kind::int8, kind::int8, kind::int8>();
    auto key = jogasaki::mock::create_nullable_record<kind::int8>();
    auto value = jogasaki::mock::create_nullable_record<kind::int8>();
    auto key_meta = key.record_meta();
    auto value_meta = value.record_meta();
    auto g_meta = group_meta{key_meta, value_meta};
    auto tmeta = tgt.record_meta();
    variable_table_info block_info{
        {
            { g0v0, { tmeta->value_offset(0), tmeta->nullity_offset(0), 0} },
            { g0v2, { tmeta->value_offset(1), tmeta->nullity_offset(1), 1} },
            { g1v0, { tmeta->value_offset(2), tmeta->nullity_offset(2), 2} },
            { g1v2, { tmeta->value_offset(3), tmeta->nullity_offset(3), 3} },
        },
        tmeta,
    };
    variable_table variables{block_info};

    std::vector<ops::group_element> groups{};
    groups.emplace_back(
        order0,
        maybe_shared_ptr(&g_meta),
        r0.groups()[0].columns(),
        0,
        block_info
    );
    groups.emplace_back(
        order1,
        maybe_shared_ptr(&g_meta),
        r0.groups()[1].columns(),
        1,
        block_info
    );

    using join_kind = relation::step::join::operator_kind_type;

    using iterator = mock::iterable_group_store::iterator;
    auto d = std::make_unique<verifier>();
    auto downstream = d.get();

    join<iterator> j{
        0,
        p_info,
        0,
        join_kind::left_outer,
        r1.condition(),
        std::move(d)
    };

    mock::task_context task_ctx{
        {},
        {},
        {},
        {},
    };

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    memory::lifo_paged_memory_resource varlen_resource{&pool};
    join_context ctx(
        &task_ctx,
        variables,
        &resource,
        &varlen_resource
    );

    std::vector<jogasaki::mock::basic_record> result{};

    downstream->body([&]() {
        result.emplace_back(jogasaki::mock::basic_record(variables.store().ref(), tmeta));
    });

    mock::iterable_group_store ge1{
        jogasaki::mock::create_nullable_record<kind::int8>(3),
        {
            jogasaki::mock::create_nullable_record<kind::int8>(300),
        }
    };
    mock::iterable_group_store ge2{
        jogasaki::mock::create_nullable_record<kind::int8>(3),
        {
            jogasaki::mock::create_nullable_record<kind::int8>(200),
            jogasaki::mock::create_nullable_record<kind::int8>(201),
            jogasaki::mock::create_nullable_record<kind::int8>(202),
        }
    };

    std::vector<group_field> fields[2];
    std::size_t tgt_field = 0;
    for(std::size_t loop = 0; loop < 2; ++loop) { // left then right
        for(std::size_t i=0, n=key.record_meta()->field_count() ; i < n; ++i) {
            auto& meta = key.record_meta();
            fields[loop].emplace_back(
                meta->at(i),
                meta->value_offset(i),
                tgt.record_meta()->value_offset(tgt_field),
                meta->nullity_offset(i),
                tgt.record_meta()->nullity_offset(tgt_field),
                true,
                true
            );
            ++tgt_field;
        }
        for(std::size_t i=0, n=value.record_meta()->field_count() ; i < n; ++i) {
            auto& meta = value.record_meta();
            fields[loop].emplace_back(
                meta->at(i),
                meta->value_offset(i),
                tgt.record_meta()->value_offset(tgt_field),
                meta->nullity_offset(i),
                tgt.record_meta()->nullity_offset(tgt_field),
                true,
                false
            );
            ++tgt_field;
        }
    }
    using iterator_pair = utils::iterator_pair<iterator>;
    std::vector<ops::group<iterator>> mygroups{
        group{
            iterator_pair{
                ge1.begin(),
                ge1.end()
            },
            fields[0],
            ge1.key().ref(),
            ge1.values()[0].record_meta()->record_size()
        },
        group{
            iterator_pair{
                ge2.begin(),
                ge2.end()
            },
            fields[1],
            ge2.key().ref(),
            value_meta->record_size()
            // ge2.values()[0].record_meta()->record_size()
        }
    };
    cogroup<iterator> mycgrp{
        mygroups
    };
    j(ctx, mycgrp);

    ASSERT_EQ(1, result.size());
    std::vector<jogasaki::mock::basic_record> exp{
        jogasaki::mock::create_nullable_record<kind::int8, kind::int8, kind::int8, kind::int8>(
            {3,300,-1,-1},
            {false, false, true, true}),
    };
    std::sort(exp.begin(), exp.end());
    std::sort(result.begin(), result.end());
    ASSERT_EQ(exp, result);
    ctx.release();
}
}

