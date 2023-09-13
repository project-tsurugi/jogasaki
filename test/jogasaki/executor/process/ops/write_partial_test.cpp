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
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/iterator.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/operator_test_utils.h>
#include <jogasaki/kvs_test_base.h>

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

class write_partial_test :
    public test_root,
    public kvs_test_base,
    public operator_test_utils {
public:
    void SetUp() override {
        kvs_db_setup();
    }
    void TearDown() override {
        kvs_db_teardown();
    }

    void show_record(
        meta::record_meta const& meta,
        std::string_view data
    ) {
        std::string in{data};
        kvs::readable_stream key_stream{in};
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
        kvs::writable_stream key_stream{key_buf};
        kvs::writable_stream val_stream{val_buf};

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
    std::shared_ptr<table> t1_ = create_table({
        "T1",
        {
            { "C0", t::int4(), nullity{false} },
            { "C1", t::float8(), nullity{false} },
            { "C2", t::int8(), nullity{false} },
        },
    });
    std::shared_ptr<index> i1_ = create_primary_index(t1_, {0}, {1,2});

    std::shared_ptr<table> t1nullable_ = create_table({
        "T1NULLABLE",
        {
            { "C0", t::int4(), nullity{true} },
            { "C1", t::float8(), nullity{true} },
            { "C2", t::int8(), nullity{true} },
        },
    });
    std::shared_ptr<index> i1nullable_ = create_primary_index(t1nullable_, {0}, {1,2});

    std::shared_ptr<table> t100_ = create_table({
        "T100",
        {
            { "C0", t::int8(), nullity{false} },
            { "C1", t::int8(), nullity{false} },
            { "C2", t::int8(), nullity{false} },
            { "C3", t::int8(), nullity{false} },
        },
    });
    std::shared_ptr<index> i100_ = create_primary_index(t100_, {0}, {1,2,3});
    std::shared_ptr<index> i100_secondary_ = create_secondary_index(t100_, "T100_SECONDARY_", {1}, {});

    auto create_target(
        relation::step::take_flat& take,
        relation::write::operator_kind_type operator_kind,
        std::shared_ptr<index> idx,
        std::shared_ptr<table> tbl,
        std::initializer_list<std::size_t> key_indices,
        std::initializer_list<std::size_t> column_indices
    ) -> relation::write& {
        std::vector<relation::write::key> keys{};
        for(auto i : key_indices) {
            keys.emplace_back(take.columns()[i].destination(), bindings_(tbl->columns()[i]));
        }
        std::vector<relation::write::column> columns{};
        for(auto i : column_indices) {
            columns.emplace_back(take.columns()[i].destination(), bindings_(tbl->columns()[i]));
        }
        return process_.operators().insert(relation::write {
            operator_kind,
            bindings_(*idx),
            std::move(keys),
            std::move(columns)
        });
    }
    std::pair<relation::step::take_flat&, relation::write&> create_update_take_target_i1() {
        auto& take = add_take(3);
        add_column_types(take, t::int4{}, t::float8{}, t::int8{});
        auto& target = create_target(take, relation::write_kind::update, i1_, t1_, {0}, {2});
        take.output() >> target.input();
        add_key_types(target, t::int4{});
        add_column_types(target, t::int8{});
        return {take, target};
    }
    std::pair<relation::step::take_flat&, relation::write&> create_update_take_target_i1_nullable() {
        auto& take = add_take(3);
        add_column_types(take, t::int4{}, t::float8{}, t::int8{});
        auto& target = create_target(take, relation::write_kind::update, i1nullable_, t1nullable_, {0}, {2});
        take.output() >> target.input();
        add_key_types(target, t::int4{});
        add_column_types(target, t::int8{});
        return {take, target};
    }
    std::pair<relation::step::take_flat&, relation::write&> create_update_multi_take_target_i1() {
        auto& take = add_take(3);
        add_column_types(take, t::int4{}, t::float8{}, t::int8{});
        auto& target = create_target(take, relation::write_kind::update, i1_, t1_, {0}, {2, 1});
        take.output() >> target.input();
        add_key_types(target, t::int4{});
        add_column_types(target, t::int8{}, t::float8{});
        return {take, target};
    }
    std::pair<relation::step::take_flat&, relation::write&> create_update_take_target_i100() {
        auto& take = add_take(4);
        add_column_types(take, t::int8{}, t::int8{}, t::int8{}, t::int8{});
        auto& target = create_target(take, relation::write_kind::update, i100_, t100_, {0}, {1});
        take.output() >> target.input();
        add_key_types(target, t::int8{});
        add_column_types(target, t::int8{});
        return {take, target};
    }
};

TEST_F(write_partial_test , simple_update) {
    auto&& [take, target] = create_update_take_target_i1();
    create_processor_info();
    auto input = create_nullable_record<kind::int4, kind::int8>(10, 1000);
    auto vars = sources(target.keys());
    vars.emplace_back(sources(target.columns())[0]);
    variable_table_info input_variable_info{create_variable_table_info(vars, input)};
    variable_table input_variables{input_variable_info};
    input_variables.store().set(input.ref());

    write_partial wrt{
        0,
        *processor_info_,
        0,
        write_kind::update,
        *i1_,
        target.keys(),
        target.columns(),
        &input_variable_info
    };

    mock::task_context task_ctx{};
    put( *db_, i1_->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db_, i1_->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));

    auto tx = wrap(db_->create_transaction());
    auto stg = db_->get_storage(i1_->simple_name());
    lifo_paged_memory_resource resource{&global::page_pool()};
    lifo_paged_memory_resource varlen_resource{&global::page_pool()};

    write_partial_context ctx{
        &task_ctx,
        input_variables,
        std::move(stg),
        tx.get(),
        wrt.primary().key_meta(),
        wrt.primary().value_meta(),
        &resource,
        &varlen_resource,
        {}
    };

    ASSERT_TRUE(static_cast<bool>(wrt(ctx)));
    (void)tx->commit();

    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1_->simple_name(), create_record<kind::int4>(0), create_record<kind::float8, kind::int8>(0.0, 0), result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ(create_record<kind::int4>(10), result[0].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(1.0, 1000)), result[0].second);
    EXPECT_EQ(create_record<kind::int4>(20), result[1].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(2.0, 200)), result[1].second);
}

TEST_F(write_partial_test , nullable_columns) {
    auto&& [take, target] = create_update_take_target_i1_nullable();
    create_processor_info();
    auto input = create_nullable_record<kind::int4, kind::int8>(10, 1000);
    auto vars = sources(target.keys());
    vars.emplace_back(sources(target.columns())[0]);
    variable_table_info input_variable_info{create_variable_table_info(vars, input)};
    variable_table input_variables{input_variable_info};
    input_variables.store().set(input.ref());

    write_partial wrt{
        0,
        *processor_info_,
        0,
        write_kind::update,
        *i1nullable_,
        target.keys(),
        target.columns(),
        &input_variable_info
    };

    mock::task_context task_ctx{};
    put( *db_, i1nullable_->simple_name(), create_nullable_record<kind::int4>(10), create_nullable_record<kind::float8, kind::int8>(1.0, 100));
    put( *db_, i1nullable_->simple_name(), create_nullable_record<kind::int4>(20), create_nullable_record<kind::float8, kind::int8>(2.0, 200));

    auto tx = wrap(db_->create_transaction());
    auto stg = db_->get_storage(i1nullable_->simple_name());
    lifo_paged_memory_resource resource{&global::page_pool()};
    lifo_paged_memory_resource varlen_resource{&global::page_pool()};

    write_partial_context ctx{
        &task_ctx,
        input_variables,
        std::move(stg),
        tx.get(),
        wrt.primary().key_meta(),
        wrt.primary().value_meta(),
        &resource,
        &varlen_resource,
        {}
    };

    ASSERT_TRUE(static_cast<bool>(wrt(ctx)));
    (void)tx->commit();

    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1nullable_->simple_name(), create_nullable_record<kind::int4>(0), create_nullable_record<kind::float8, kind::int8>(0.0, 0), result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ(create_record<kind::int4>(10), result[0].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(1.0, 1000)), result[0].second);
    EXPECT_EQ(create_record<kind::int4>(20), result[1].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(2.0, 200)), result[1].second);
}

TEST_F(write_partial_test , update_multi_columns) {
    auto&& [take, target] = create_update_multi_take_target_i1();
    create_processor_info();
    auto input = create_nullable_record<kind::int4, kind::int8, kind::float8>(10, 1000, 10000.0);
    auto vars = sources(target.keys());
    vars.emplace_back(sources(target.columns())[0]);
    vars.emplace_back(sources(target.columns())[1]);
    variable_table_info input_variable_info{create_variable_table_info(vars, input)};
    variable_table input_variables{input_variable_info};
    input_variables.store().set(input.ref());

    write_partial wrt{
        0,
        *processor_info_,
        0,
        write_kind::update,
        *i1_,
        target.keys(),
        target.columns(),
        &input_variable_info
    };

    mock::task_context task_ctx{};
    put( *db_, i1_->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db_, i1_->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));

    auto tx = wrap(db_->create_transaction());
    auto stg = db_->get_storage(i1_->simple_name());
    lifo_paged_memory_resource resource{&global::page_pool()};
    lifo_paged_memory_resource varlen_resource{&global::page_pool()};

    write_partial_context ctx{
        &task_ctx,
        input_variables,
        std::move(stg),
        tx.get(),
        wrt.primary().key_meta(),
        wrt.primary().value_meta(),
        &resource,
        &varlen_resource,
        {}
    };

    ASSERT_TRUE(static_cast<bool>(wrt(ctx)));
    (void)tx->commit();

    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1_->simple_name(), create_record<kind::int4>(0), create_record<kind::float8, kind::int8>(0.0, 0), result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ(create_record<kind::int4>(10), result[0].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(10000.0, 1000)), result[0].second);
    EXPECT_EQ(create_record<kind::int4>(20), result[1].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(2.0, 200)), result[1].second);
}

TEST_F(write_partial_test , update_secondary) {
    auto&& [take, target] = create_update_take_target_i100();
    create_processor_info();
    auto input = create_nullable_record<kind::int8, kind::int8>(10, 10000);
    auto vars = sources(target.keys());
    vars.emplace_back(sources(target.columns())[0]);

    variable_table_info input_variable_info{create_variable_table_info(vars, input)};
    variable_table input_variables{input_variable_info};
    input_variables.store().set(input.ref());

    write_partial wrt{
        0,
        *processor_info_,
        0,
        write_kind::update,
        *i100_,
        target.keys(),
        target.columns(),
        &input_variable_info
    };

    mock::task_context task_ctx{};
    {
        auto pkey = put( *db_, i100_->simple_name(), create_record<kind::int8>(10), create_record<kind::int8, kind::int8, kind::int8>(1, 100, 1000));
        put_secondary(*db_, i100_secondary_->simple_name(), create_record<kind::int8>(1), pkey);
    }
    {
        auto pkey = put( *db_, i100_->simple_name(), create_record<kind::int8>(20), create_record<kind::int8, kind::int8, kind::int8>(2, 200, 2000));
        put_secondary(*db_, i100_secondary_->simple_name(), create_record<kind::int8>(2), pkey);
    }

    auto tx = wrap(db_->create_transaction());
    auto stg = db_->get_storage(i100_->simple_name());
    lifo_paged_memory_resource resource{&global::page_pool()};
    lifo_paged_memory_resource varlen_resource{&global::page_pool()};

    std::vector<details::write_secondary_context> secondaries{};
    secondaries.emplace_back(
        db_->get_or_create_storage(i100_secondary_->simple_name()),
        nullptr
    );

    write_partial_context ctx{
        &task_ctx,
        input_variables,
        std::move(stg),
        tx.get(),
        wrt.primary().key_meta(),
        wrt.primary().value_meta(),
        &resource,
        &varlen_resource,
        std::move(secondaries)

    };

    ASSERT_TRUE(static_cast<bool>(wrt(ctx)));
    (void)tx->commit();

    {
        std::vector<std::pair<basic_record, basic_record>> result{};
        get(*db_, i100_->simple_name(), create_record<kind::int8>(0), create_record<kind::int8, kind::int8, kind::int8>(0, 0, 0), result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ(create_record<kind::int8>(10), result[0].first);
        EXPECT_EQ((create_record<kind::int8, kind::int8, kind::int8>(10000, 100, 1000)), result[0].second);
        EXPECT_EQ(create_record<kind::int8>(20), result[1].first);
        EXPECT_EQ((create_record<kind::int8, kind::int8, kind::int8>(2, 200, 2000)), result[1].second);
    }
}

}

