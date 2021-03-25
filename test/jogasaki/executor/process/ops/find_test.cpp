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
#include <jogasaki/executor/process/impl/ops/find.h>

#include <string>

#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <takatori/type/character.h>
#include <takatori/value/character.h>
#include <takatori/type/int.h>
#include <takatori/value/int.h>
#include <takatori/util/object_creator.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/executor/process/impl/ops/find_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/executor/process/impl/ops/operator_builder.h>
#include "verifier.h"

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace jogasaki::mock;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;
using take = relation::step::take_flat;
using buffer = relation::buffer;

namespace storage = yugawara::storage;

using yugawara::variable::nullity;
using yugawara::variable::criteria;

class find_test : public test_root {
public:
    static constexpr kvs::order undef = kvs::order::undefined;
    static constexpr kvs::order asc = kvs::order::ascending;
    static constexpr kvs::order desc = kvs::order::descending;

    static constexpr kvs::coding_spec spec_asc = kvs::spec_key_ascending;
    static constexpr kvs::coding_spec spec_desc = kvs::spec_key_descending;
    static constexpr kvs::coding_spec spec_val = kvs::spec_value;
    basic_record create_key(
        std::int32_t arg0
    ) {
        return create_record<kind::int4>(arg0);
    }

    basic_record create_value(
        double arg0,
        std::int64_t arg1
    ) {
        return create_record<kind::float8, kind::int8>(arg0, arg1);
    }

    basic_record create_nullable_value(
        double arg0,
        std::int64_t arg1,
        bool arg0_null,
        bool arg1_null
    ) {
        return create_nullable_record<kind::float8, kind::int8>(std::forward_as_tuple(arg0, arg1), {arg0_null, arg1_null});
    }
};

TEST_F(find_test, simple) {

    binding::factory bindings;
    std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<storage::table> t0 = storages->add_table({
        "T0",
        {
            { "C0", t::int4(), nullity{false} },
            { "C1", t::float8(), nullity{false}  },
            { "C2", t::int8(), nullity{false}  },
        },
    });
    storage::column const& t0c0 = t0->columns()[0];
    storage::column const& t0c1 = t0->columns()[1];
    storage::column const& t0c2 = t0->columns()[2];

    std::shared_ptr<::yugawara::storage::index> i0 = storages->add_index({
        t0,
        "I0",
        {
            t0->columns()[0],
        },
        {
            t0->columns()[1],
            t0->columns()[2],
        },
        {
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        },
    });

    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});
    auto c0 = bindings.stream_variable("c0");
    auto c1 = bindings.stream_variable("c1");
    auto c2 = bindings.stream_variable("c2");

    auto v0 = bindings(t0c0);
    auto v1 = bindings(t0c1);
    auto v2 = bindings(t0c2);
    auto& r0 = p0.operators().insert(relation::find {
        bindings(*i0),
        {
            { v0, c0 },
            { v1, c1 },
            { v2, c2 },
        },
        {
            relation::find::key{
                c0,
                scalar::immediate{takatori::value::int4(20), takatori::type::int4()}
            }
        }
    });


    ::takatori::plan::forward f1 {
        bindings.exchange_column(),
        bindings.exchange_column(),
        bindings.exchange_column(),
    };
    auto&& f1c0 = f1.columns()[0];
    auto&& f1c1 = f1.columns()[1];
    auto&& f1c2 = f1.columns()[2];
    // without offer, the columns are not used and block variables become empty
    auto&& r1 = p0.operators().insert(relation::step::offer {
        bindings.exchange(f1),
        {
            { c0, f1c0 },
            { c1, f1c1 },
            { c2, f1c2 },
        },
    });
    r0.output() >> r1.input(); // connection required by takatori

    auto vmap = std::make_shared<yugawara::analyzer::variable_mapping>();
    vmap->bind(v0, t::int4{});
    vmap->bind(v1, t::float8{});
    vmap->bind(v2, t::int8{});
    vmap->bind(c0, t::int4{});
    vmap->bind(c1, t::float8{});
    vmap->bind(c2, t::int8{});
    auto emap = std::make_shared<yugawara::analyzer::expression_mapping>();
    emap->bind(r0.keys()[0].value(), t::int4{});
    yugawara::compiled_info c_info{emap, vmap};

    processor_info p_info{p0.operators(), c_info};

    std::vector<find::column, takatori::util::object_allocator<find::column>> find_columns{
        {v0, c0},
        {v1, c1},
        {v2, c2},
    };

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    memory::lifo_paged_memory_resource varlen_resource{&global::page_pool()};
    using kind = meta::field_type_kind;
    auto d = std::make_unique<verifier>();
    auto downstream = d.get();
    find s{
        0,
        p_info,
        0,
        "I0"sv,
        operator_builder::encode_key<relation::find::key>(r0.keys(), i0->keys(), p_info, resource),
        *i0,
        find_columns,
        std::move(d)
    };

    auto& block_info = p_info.vars_info_list()[s.block_index()];
    variable_table variables{block_info};

    mock::task_context task_ctx{
        {},
        {},
        {},
        {},
    };


    auto db = kvs::database::open();
    auto stg = db->create_storage("I0");
    auto tx = db->create_transaction();

    std::string key_buf(100, '\0');
    std::string val_buf(100, '\0');
    kvs::stream key_stream{key_buf};
    kvs::stream val_stream{val_buf};

    using key_record = jogasaki::mock::basic_record;
    using value_record = jogasaki::mock::basic_record;
    {
        key_record key_rec{create_key(10)};
        auto key_meta = key_rec.record_meta();
        kvs::encode(key_rec.ref(), key_meta->value_offset(0), key_meta->at(0), spec_asc, key_stream);
        value_record val_rec{create_value(1.0, 100)};
        auto val_meta = val_rec.record_meta();
        kvs::encode(val_rec.ref(), val_meta->value_offset(0), val_meta->at(0), spec_val, val_stream);
        kvs::encode(val_rec.ref(), val_meta->value_offset(1), val_meta->at(1), spec_val, val_stream);
        ASSERT_EQ(status::ok, stg->put(*tx,
            std::string_view{key_buf.data(), key_stream.length()},
            std::string_view{val_buf.data(), val_stream.length()}
        ));
    }
    key_stream.reset();
    val_stream.reset();
    {
        key_record key_rec{create_key(20)};
        auto key_meta = key_rec.record_meta();
        kvs::encode(key_rec.ref(), key_meta->value_offset(0), key_meta->at(0), spec_asc, key_stream);
        value_record val_rec{create_value(2.0, 200)};
        auto val_meta = val_rec.record_meta();
        kvs::encode(val_rec.ref(), val_meta->value_offset(0), val_meta->at(0), spec_val, val_stream);
        kvs::encode(val_rec.ref(), val_meta->value_offset(1), val_meta->at(1), spec_val, val_stream);
        ASSERT_EQ(status::ok, stg->put(*tx,
            std::string_view{key_buf.data(), key_stream.length()},
            std::string_view{val_buf.data(), val_stream.length()}
        ));
    }
    ASSERT_EQ(status::ok, tx->commit());

    auto tx2 = db->create_transaction();
    auto t = tx2.get();
    find_context ctx(&task_ctx, variables, std::move(stg), t, &resource, &varlen_resource);

    auto vars_ref = variables.store().ref();
    auto map = variables.value_map();
    auto vars_meta = variables.meta();

    auto c0_offset = map.at(c0).value_offset();
    auto c1_offset = map.at(c1).value_offset();
    auto c2_offset = map.at(c2).value_offset();

    std::size_t count = 0;
    downstream->body([&]() {
        switch(count) {
            case 0: {
                EXPECT_EQ(20, vars_ref.get_value<std::int32_t>(c0_offset));
                EXPECT_DOUBLE_EQ(2.0, vars_ref.get_value<double>(c1_offset));
                EXPECT_EQ(200, vars_ref.get_value<std::int64_t>(c2_offset));
                break;
            }
            default:
                ADD_FAILURE();
        }
        ++count;
    });
    s(ctx);
    ctx.release();
    ASSERT_EQ(1, count);
    (void)t->abort();
    (void)db->close();
}

}

