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
#include <jogasaki/executor/process/impl/ops/scan.h>

#include <string>

#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <takatori/type/character.h>
#include <takatori/value/character.h>
#include <takatori/util/object_creator.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/executor/process/impl/ops/scan_context.h>
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

class scan_test : public test_root {
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

TEST_F(scan_test, simple) {

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
    auto& r0 = p0.operators().insert(relation::scan {
        bindings(*i0),
        {
            { v0, c0 },
            { v1, c1 },
            { v2, c2 },
        },
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
    yugawara::compiled_info c_info{{}, vmap};

    processor_info p_info{p0.operators(), c_info};

    std::vector<scan::column, takatori::util::object_allocator<scan::column>> scan_columns{
        {v0, c0},
        {v1, c1},
        {v2, c2},
    };
    using kind = meta::field_type_kind;
    auto d = std::make_unique<verifier>();
    auto downstream = d.get();
    scan s{
        0,
        p_info,
        0,
        "I0"sv,
        *i0,
        scan_columns,
        std::move(d)
    };

    auto& block_info = p_info.vars_info_list()[s.block_index()];
    variable_table variables{block_info};

    auto sinfo = std::make_shared<impl::scan_info>();
    mock::task_context task_ctx{
        {},
        {},
        {},
        {sinfo},
    };

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    memory::lifo_paged_memory_resource varlen_resource{&global::page_pool()};

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
    scan_context ctx(&task_ctx, variables, std::move(stg), t, sinfo.get(), &resource, &varlen_resource);

    auto vars_ref = variables.store().ref();
    auto& map = variables.info();
    auto vars_meta = variables.meta();

    auto c0_offset = map.at(c0).value_offset();
    auto c1_offset = map.at(c1).value_offset();
    auto c2_offset = map.at(c2).value_offset();

    std::size_t count = 0;
    downstream->body([&]() {
        switch(count) {
            case 0: {
                EXPECT_EQ(10, vars_ref.get_value<std::int32_t>(c0_offset));
                EXPECT_DOUBLE_EQ(1.0, vars_ref.get_value<double>(c1_offset));
                EXPECT_EQ(100, vars_ref.get_value<std::int64_t>(c2_offset));
                break;
            }
            case 1: {
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
    ASSERT_EQ(2, count);
    (void)t->abort();
    (void)db->close();
}


TEST_F(scan_test, nullable_fields) {

    binding::factory bindings;
    std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<storage::table> t0 = storages->add_table({
        "T0",
        {
            { "C0", t::int4(), nullity{false} },
            { "C1", t::float8(), nullity{true}  },
            { "C2", t::int8(), nullity{true}  },
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
    auto& r0 = p0.operators().insert(relation::scan {
        bindings(*i0),
        {
            { v0, c0 },
            { v1, c1 },
            { v2, c2 },
        },
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
    yugawara::compiled_info c_info{{}, vmap};

    processor_info p_info{p0.operators(), c_info};

    std::vector<scan::column, takatori::util::object_allocator<scan::column>> scan_columns{
        {v0, c0},
        {v1, c1},
        {v2, c2},
    };
    using kind = meta::field_type_kind;
    auto d = std::make_unique<verifier>();
    auto downstream = d.get();
    scan s{
        0,
        p_info,
        0,
        "I0"sv,
        *i0,
        scan_columns,
        std::move(d)
    };

    auto& block_info = p_info.vars_info_list()[s.block_index()];
    variable_table variables{block_info};

    auto sinfo = std::make_shared<impl::scan_info>();
    mock::task_context task_ctx{
        {},
        {},
        {},
        {sinfo},
    };

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    memory::lifo_paged_memory_resource varlen_resource{&global::page_pool()};

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
        value_record val_rec{create_nullable_value(1.0, 100, false, false)};
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
        kvs::encode(key_rec.ref(), key_meta->value_offset(0), key_meta->at(0), spec_asc, key_stream);
        value_record val_rec{create_nullable_value(0.0, 0, true, true)};
        auto val_meta = val_rec.record_meta();
        kvs::encode_nullable(val_rec.ref(), val_meta->value_offset(0), val_meta->nullity_offset(0), val_meta->at(0), spec_val, val_stream);
        kvs::encode_nullable(val_rec.ref(), val_meta->value_offset(1), val_meta->nullity_offset(1), val_meta->at(1), spec_val, val_stream);
        ASSERT_EQ(status::ok, stg->put(*tx,
            std::string_view{key_buf.data(), key_stream.length()},
            std::string_view{val_buf.data(), val_stream.length()}
        ));
    }
    ASSERT_EQ(status::ok, tx->commit());

    auto tx2 = db->create_transaction();
    auto t = tx2.get();
    scan_context ctx(&task_ctx, variables, std::move(stg), t, sinfo.get(), &resource, &varlen_resource);

    auto vars_ref = variables.store().ref();
    auto& map = variables.info();
    auto vars_meta = variables.meta();

    auto c0_offset = map.at(c0).value_offset();
    auto c1_offset = map.at(c1).value_offset();
    auto c2_offset = map.at(c2).value_offset();
    auto c1_nullity_offset = map.at(c1).nullity_offset();
    auto c2_nullity_offset = map.at(c2).nullity_offset();

    std::size_t count = 0;
    downstream->body([&]() {
        switch(count) {
            case 0: {
                EXPECT_EQ(10, vars_ref.get_value<std::int32_t>(c0_offset));
                EXPECT_DOUBLE_EQ(1.0, *vars_ref.get_if<double>(c1_nullity_offset, c1_offset));
                EXPECT_EQ(100, *vars_ref.get_if<std::int64_t>(c2_nullity_offset, c2_offset));
                break;
            }
            case 1: {
                EXPECT_EQ(20, vars_ref.get_value<std::int32_t>(c0_offset));
                EXPECT_FALSE(vars_ref.get_if<double>(c1_nullity_offset, c1_offset));
                EXPECT_FALSE(vars_ref.get_if<std::int64_t>(c2_nullity_offset, c2_offset));
                break;
            }
            default:
                ADD_FAILURE();
        }
        ++count;
    });
    s(ctx);
    ctx.release();
    ASSERT_EQ(2, count);
    (void)t->abort();
    (void)db->close();
}

TEST_F(scan_test, scan_info) {

    binding::factory bindings;
    std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<storage::table> t1 = storages->add_table({
        "T1",
        {
            { "C0", t::int8(), nullity{false} },
            { "C1", t::character(t::varying, 100), nullity{false}  },
            { "C2", t::float8(), nullity{false}  },
        },
    });
    storage::column const& t1c0 = t1->columns()[0];
    storage::column const& t1c1 = t1->columns()[1];
    storage::column const& t1c2 = t1->columns()[2];

    std::shared_ptr<::yugawara::storage::index> i1 = storages->add_index({
        t1,
        "I1",
        {
            t1->columns()[0],
            t1->columns()[1],
        },
        {
            t1->columns()[2],
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

    auto v0 = bindings(t1c0);
    auto v1 = bindings(t1c1);
    auto v2 = bindings(t1c2);
    using key = relation::scan::key;
    auto& r0 = p0.operators().insert(relation::scan {
        bindings(*i1),
        {
            { v0, c0 },
            { v1, c1 },
            { v2, c2 },
        },
        {
            {
                key {
                    v0,
                    scalar::immediate { takatori::value::int8(100), takatori::type::int8() }
                },
                key {
                    v1,
                    scalar::immediate { takatori::value::character("123456789012345678901234567890/B"), takatori::type::character(t::varying, 100) }
                },
            },
            relation::endpoint_kind::inclusive,
        },
        {
            {
                key {
                    v0,
                    scalar::immediate { takatori::value::int8(100), takatori::type::int8() }
                },
                key {
                    v1,
                    scalar::immediate { takatori::value::character("123456789012345678901234567890/D"), takatori::type::character(t::varying, 100) }
                },
            },
            relation::endpoint_kind::exclusive,
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
    vmap->bind(v0, t::int8{});
    vmap->bind(v1, t::character{t::varying, 100});
    vmap->bind(v2, t::float8{});
    vmap->bind(c0, t::int8{});
    vmap->bind(c1, t::character{t::varying, 100});
    vmap->bind(c2, t::float8{});

    auto emap = std::make_shared<yugawara::analyzer::expression_mapping>();
    emap->bind(r0.lower().keys()[0].value(), t::int8{});
    emap->bind(r0.lower().keys()[1].value(), t::character{t::varying, 100});
    emap->bind(r0.upper().keys()[0].value(), t::int8{});
    emap->bind(r0.upper().keys()[1].value(), t::character{t::varying, 100});
    yugawara::compiled_info c_info{emap, vmap};

    auto p_info = std::make_shared<processor_info>(p0.operators(), c_info);

    std::vector<scan::column, takatori::util::object_allocator<scan::column>> scan_columns{
        {v0, c0},
        {v1, c1},
        {v2, c2},
    };
    using kind = meta::field_type_kind;
    auto d = std::make_unique<verifier>();
    auto downstream = d.get();
    scan s{
        0,
        *p_info,
        0,
        "I1"sv,
        *i1,
        scan_columns,
        std::move(d)
    };

    auto& block_info = p_info->vars_info_list()[s.block_index()];
    variable_table variables{block_info};

    memory::lifo_paged_memory_resource resource{&global::page_pool()};
    memory::lifo_paged_memory_resource varlen_resource{&global::page_pool()};

    jogasaki::plan::compiler_context compiler_ctx{};
    io_exchange_map exchange_map{};
    operator_builder builder{p_info, {}, {}, exchange_map, &resource};
    auto sinfo = builder.create_scan_info(r0, i1->keys());
    mock::task_context task_ctx{
        {},
        {},
        {},
        {sinfo},
    };

    auto db = kvs::database::open();
    auto stg = db->create_storage("I1");
    auto tx = db->create_transaction();

    std::string key_buf(100, '\0');
    std::string val_buf(100, '\0');
    kvs::stream key_stream{key_buf};
    kvs::stream val_stream{val_buf};

    using key_record = jogasaki::mock::basic_record;
    using value_record = jogasaki::mock::basic_record;
    {
        key_record key_rec{create_record<kind::int8, kind::character>(100, accessor::text{"123456789012345678901234567890/B"})};
        auto key_meta = key_rec.record_meta();
        kvs::encode(key_rec.ref(), key_meta->value_offset(0), key_meta->at(0), spec_asc, key_stream);
        kvs::encode(key_rec.ref(), key_meta->value_offset(1), key_meta->at(1), spec_asc, key_stream);
        value_record val_rec{create_record<kind::float8>(1.0)};
        auto val_meta = val_rec.record_meta();
        kvs::encode(val_rec.ref(), val_meta->value_offset(0), val_meta->at(0), spec_val, val_stream);
        ASSERT_EQ(status::ok, stg->put(*tx,
            std::string_view{key_buf.data(), key_stream.length()},
            std::string_view{val_buf.data(), val_stream.length()}
        ));
    }
    key_stream.reset();
    val_stream.reset();
    {
        key_record key_rec{create_record<kind::int8, kind::character>(100, accessor::text{"123456789012345678901234567890/C"})};
        auto key_meta = key_rec.record_meta();
        kvs::encode(key_rec.ref(), key_meta->value_offset(0), key_meta->at(0), spec_asc, key_stream);
        kvs::encode(key_rec.ref(), key_meta->value_offset(1), key_meta->at(1), spec_asc, key_stream);
        value_record val_rec{create_record<kind::float8>(2.0)};
        auto val_meta = val_rec.record_meta();
        kvs::encode(val_rec.ref(), val_meta->value_offset(0), val_meta->at(0), spec_val, val_stream);
        ASSERT_EQ(status::ok, stg->put(*tx,
            std::string_view{key_buf.data(), key_stream.length()},
            std::string_view{val_buf.data(), val_stream.length()}
        ));
    }
    key_stream.reset();
    val_stream.reset();
    {
        key_record key_rec{create_record<kind::int8, kind::character>(100, accessor::text{"123456789012345678901234567890/D"})};
        auto key_meta = key_rec.record_meta();
        kvs::encode(key_rec.ref(), key_meta->value_offset(0), key_meta->at(0), spec_asc, key_stream);
        kvs::encode(key_rec.ref(), key_meta->value_offset(1), key_meta->at(1), spec_asc, key_stream);
        value_record val_rec{create_record<kind::float8>(3.0)};
        auto val_meta = val_rec.record_meta();
        kvs::encode(val_rec.ref(), val_meta->value_offset(0), val_meta->at(0), spec_val, val_stream);
        ASSERT_EQ(status::ok, stg->put(*tx,
            std::string_view{key_buf.data(), key_stream.length()},
            std::string_view{val_buf.data(), val_stream.length()}
        ));
    }
    ASSERT_EQ(status::ok, tx->commit());

    auto tx2 = db->create_transaction();
    auto t = tx2.get();
    scan_context ctx(&task_ctx, variables, std::move(stg), t, sinfo.get(), &resource, &varlen_resource);

    auto vars_ref = variables.store().ref();
    auto& map = variables.info();
    auto vars_meta = variables.meta();

    auto c0_offset = map.at(c0).value_offset();
    auto c1_offset = map.at(c1).value_offset();
    auto c2_offset = map.at(c2).value_offset();

    std::size_t count = 0;
    downstream->body([&]() {
        switch(count) {
            case 0: {
                EXPECT_EQ(100, vars_ref.get_value<std::int64_t>(c0_offset));
                EXPECT_EQ(accessor::text("123456789012345678901234567890/B"), vars_ref.get_value<accessor::text>(c1_offset));
                EXPECT_DOUBLE_EQ(1.0, vars_ref.get_value<double>(c2_offset));
                break;
            }
            case 1: {
                EXPECT_EQ(100, vars_ref.get_value<std::int64_t>(c0_offset));
                EXPECT_EQ(accessor::text("123456789012345678901234567890/C"), vars_ref.get_value<accessor::text>(c1_offset));
                EXPECT_DOUBLE_EQ(2.0, vars_ref.get_value<double>(c2_offset));
                break;
            }
            default:
                ADD_FAILURE();
        }
        ++count;
    });
    s(ctx);
    ctx.release();
    ASSERT_EQ(2, count);
    (void)t->abort();
    (void)db->close();
}

}

