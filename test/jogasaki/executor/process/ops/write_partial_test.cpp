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
#include <jogasaki/executor/process/impl/ops/write_partial.h>

#include <string>

#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/executor/process/impl/ops/write_partial_context.h>
#include <jogasaki/executor/process/impl/block_scope.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/iterator.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/mock/task_context.h>

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::mock;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;

namespace storage = yugawara::storage;

class write_partial_test : public test_root {
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
        return create_nullable_record<kind::int4>(arg0);
    }

    basic_record create_value(
        double arg0,
        std::int64_t arg1
    ) {
        return create_nullable_record<kind::float8, kind::int8>(arg0, arg1);
    }

    basic_record create_nullable_value(
        double arg0,
        std::int64_t arg1,
        bool arg0_null,
        bool arg1_null
    ) {
        return create_nullable_record<kind::float8, kind::int8>(std::forward_as_tuple(arg0, arg1), {arg0_null, arg1_null});
    }
    using key_record = jogasaki::mock::basic_record;
    using value_record = jogasaki::mock::basic_record;
    void add_data(kvs::database& db) {
        auto stg = db.create_storage("I1");
        auto tx = db.create_transaction();

        std::string key_buf(100, '\0');
        std::string val_buf(100, '\0');
        kvs::stream key_stream{key_buf};
        kvs::stream val_stream{val_buf};
        {
            key_record key_rec{create_key(10)};
            auto key_meta = key_rec.record_meta();
            kvs::encode_nullable(key_rec.ref(), key_meta->value_offset(0), key_meta->nullity_offset(0), key_meta->at(0), spec_asc, key_stream);
            value_record val_rec{create_value(1.0, 100)};
            auto val_meta = val_rec.record_meta();
            kvs::encode_nullable(val_rec.ref(), val_meta->value_offset(0), val_meta->nullity_offset(0), val_meta->at(0), spec_val, val_stream);
            kvs::encode_nullable(val_rec.ref(), val_meta->value_offset(1), val_meta->nullity_offset(1), val_meta->at(1), spec_val, val_stream);
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
            kvs::encode_nullable(key_rec.ref(), key_meta->value_offset(0), key_meta->nullity_offset(0), key_meta->at(0), spec_asc, key_stream);
            value_record val_rec{create_value(2.0, 200)};
            auto val_meta = val_rec.record_meta();
            kvs::encode_nullable(val_rec.ref(), val_meta->value_offset(0), val_meta->nullity_offset(0), val_meta->at(0), spec_val, val_stream);
            kvs::encode_nullable(val_rec.ref(), val_meta->value_offset(1), val_meta->nullity_offset(1), val_meta->at(1), spec_val, val_stream);
            ASSERT_EQ(status::ok, stg->put(*tx,
                std::string_view{key_buf.data(), key_stream.length()},
                std::string_view{val_buf.data(), val_stream.length()}
            ));
        }
        ASSERT_EQ(status::ok, tx->commit());
    }

    void show_record(
        meta::record_meta const& meta,
        std::string_view data
    ) {
        std::string in{data};
        kvs::stream key_stream{in};
        std::string out(meta.record_size(), '\0');
        accessor::record_ref target{out.data(), out.capacity()};
        for(std::size_t i=0, n=meta.field_count(); i<n; ++i) {
            kvs::decode_nullable(
                key_stream,
                meta.at(i),
                spec_asc,
                target,
                meta.value_offset(i),
                meta.nullity_offset(i)
            );
        }
        std::cout << target << meta;
    }
    void check_data(
        kvs::database& db,
        meta::record_meta const& key_meta,
        meta::record_meta const& value_meta
    ) {
        auto stg = db.get_storage("I1");
        auto tx = db.create_transaction();

        std::string key_buf(100, '\0');
        std::string val_buf(100, '\0');
        kvs::stream key_stream{key_buf};
        kvs::stream val_stream{val_buf};

        std::unique_ptr<kvs::iterator> it{};
        std::string_view k{};
        std::string_view v{};
        ASSERT_EQ(status::ok, stg->scan(*tx, "", kvs::end_point_kind::unbound, "", kvs::end_point_kind::unbound, it));
        while(it->next() == status::ok) {
            (void)it->key(k);
            (void)it->value(v);
            show_record(key_meta, k);
            show_record(value_meta, v);
        }
    }
};

TEST_F(write_partial_test , simple_update) {
    binding::factory bindings;
    std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<storage::table> t0 = storages->add_table({
        "T0",
        {
            { "C0", t::int4() },
            { "C1", t::float8() },
            { "C2", t::int8() },
        },
    });
    storage::column const& t0c0 = t0->columns()[0];
    storage::column const& t0c1 = t0->columns()[1];
    storage::column const& t0c2 = t0->columns()[2];

    std::shared_ptr<storage::index> i0 = storages->add_index(
        { t0, "I0",
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
        }
    );
    std::shared_ptr<storage::table> t1 = storages->add_table({
        "T1",
        {
            { "C0", t::int4() },
            { "C1", t::float8() },
            { "C2", t::int8() },
        },
    });
    storage::column const& t1c0 = t1->columns()[0];
    storage::column const& t1c1 = t1->columns()[1];
    storage::column const& t1c2 = t1->columns()[2];

    std::shared_ptr<storage::index> i1 = storages->add_index(
        { t1, "I1",
            {
                t1->columns()[0],
            },
            {
                t1->columns()[1],
                t1->columns()[2],
            },
            {
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
            },
        }
    );

    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});
    auto c0 = bindings.stream_variable("c0");
    auto c1 = bindings.stream_variable("c1");
    auto c2 = bindings.stream_variable("c2");
    auto& r0 = p0.operators().insert(relation::scan {
        bindings(*i0),
        {
            { bindings(t0c0), c0 },
            { bindings(t0c1), c1 },
            { bindings(t0c2), c2 },
        },
    });

    auto&& r1 = p0.operators().insert(relation::write {
        relation::write_kind::update,
        bindings(*i1),
        {
            { c0, bindings(t1c0) },
        },
        {
            { c1, bindings(t1c1) },
        },
    });

    r0.output() >> r1.input();

    auto vm = std::make_shared<yugawara::analyzer::variable_mapping>();
    vm->bind(c0, t::int4{});
    vm->bind(c1, t::float8{});
    vm->bind(c2, t::int8{});
    vm->bind(bindings(t1c0), t::int4{});
    vm->bind(bindings(t1c1), t::float8{});
    vm->bind(bindings(t1c2), t::int8{});
    vm->bind(bindings(t0c0), t::int4{});
    vm->bind(bindings(t0c1), t::float8{});
    vm->bind(bindings(t0c2), t::int8{});
    yugawara::compiled_info c_info{{}, vm};

    processor_info p_info{p0.operators(), c_info};

    std::vector<write_partial::column> write_columns{
        {c0, bindings(t1c0)},
        {c1, bindings(t1c1)},
        {c2, bindings(t1c2)},
    };

    using kind = meta::field_type_kind;
    auto meta = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(enum_tag<kind::int4>),
            field_type(enum_tag<kind::float8>),
            field_type(enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{3}.flip()
    );
    write_partial wrt{
        0,
        p_info,
        0,
        write_kind::update,
        "I1",
        *i1,
        r1.keys(),
        r1.columns()
    };

    ASSERT_EQ(1, p_info.scopes_info().size());
    auto& block_info = p_info.scopes_info()[wrt.block_index()];
    block_scope variables{block_info};

    using kind = meta::field_type_kind;
    using test_record = jogasaki::mock::basic_record;

    mock::task_context task_ctx{
        {},
        {},
        {},
        {},
    };

    auto db = kvs::database::open();
    add_data(*db);
    auto tx = db->create_transaction();
    auto stg = db->get_storage("I1");
    auto s = stg.get();

    lifo_paged_memory_resource resource{&global::page_pool()};
    lifo_paged_memory_resource varlen_resource{&global::page_pool()};
    write_partial_context ctx{
        &task_ctx,
        variables,
        std::move(stg),
        tx.get(),
        wrt.key_meta(),
        wrt.value_meta(),
        &resource,
        &varlen_resource
    };

    auto vars_ref = variables.store().ref();
    auto map = variables.value_map();
    vars_ref.set_value<std::int32_t>(map.at(c0).value_offset(), 10);
    vars_ref.set_null(map.at(c0).nullity_offset(), false);
    vars_ref.set_value<double>(map.at(c1).value_offset(), 10.0);
    vars_ref.set_null(map.at(c1).nullity_offset(), false);
    wrt(ctx);

    {
        std::string str(100, '\0');
        kvs::stream key{str};
        kvs::encode_nullable(
            expression::any{std::in_place_type<std::int32_t>, 10},
            meta::field_type{enum_tag<kind::int4>},
            kvs::coding_spec{true, kvs::order::ascending},
            key
        );
        std::string_view k{str.data(), key.length()};
        std::string_view v{};
        ASSERT_EQ(status::ok, s->get(*tx, k, v));
        std::string buf{v};
        kvs::stream value{buf};
        expression::any res{};
        kvs::decode_nullable(
            value,
            meta::field_type{enum_tag<kind::float8>},
            kvs::coding_spec{false, kvs::order::undefined},
            res
        );
        EXPECT_EQ(10.0, res.to<double>());
        kvs::decode_nullable(
            value,
            meta::field_type{enum_tag<kind::int8>},
            kvs::coding_spec{false, kvs::order::undefined},
            res
        );
        EXPECT_EQ(100, res.to<std::int64_t>());
    }
    {
        std::string str(100, '\0');
        kvs::stream key{str};
        kvs::encode_nullable(
            expression::any{std::in_place_type<std::int32_t>, 20},
            meta::field_type{enum_tag<kind::int4>},
            kvs::coding_spec{true, kvs::order::ascending},
            key
        );
        std::string_view k{str.data(), key.length()};
        std::string_view v{};
        ASSERT_EQ(status::ok, s->get(*tx, k, v));
        std::string buf{v};
        kvs::stream value{buf};
        expression::any res{};
        kvs::decode_nullable(
            value,
            meta::field_type{enum_tag<kind::float8>},
            kvs::coding_spec{false, kvs::order::undefined},
            res
        );
        EXPECT_EQ(2.0, res.to<double>());
        kvs::decode_nullable(
            value,
            meta::field_type{enum_tag<kind::int8>},
            kvs::coding_spec{false, kvs::order::undefined},
            res
        );
        EXPECT_EQ(200, res.to<std::int64_t>());
    }
}

}

