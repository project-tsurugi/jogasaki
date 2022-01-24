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
#include <jogasaki/executor/process/impl/ops/write_full.h>

#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/executor/process/impl/ops/write_full_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/iterator.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/kvs_test_utils.h>
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
using namespace std::chrono_literals;

using namespace jogasaki::mock;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;

namespace storage = yugawara::storage;

class write_full_test :
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

    std::shared_ptr<table> t1_ = create_table({
        "T1",
        {
            { "C0", t::int4(), nullity{false} },
            { "C1", t::float8(), nullity{false} },
            { "C2", t::int8(), nullity{false} },
        },
    });
    std::shared_ptr<index> i1_ = create_primary_index(t1_, {0}, {1,2});

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
    std::pair<relation::step::take_flat&, relation::write&> create_insert_take_target_i1() {
        auto& take = add_take(3);
        add_column_types(take, t::int4{}, t::float8{}, t::int8{});
        auto& target = create_target(take, relation::write_kind::insert, i1_, t1_, {}, {0,1,2});
        take.output() >> target.input();
        add_column_types(target, t::int4{}, t::float8{}, t::int8{});
        return {take, target};
    }

    std::pair<relation::step::take_flat&, relation::write&> create_delete_take_target_i1() {
        auto& take = add_take(3);
        add_column_types(take, t::int4{}, t::float8{}, t::int8{});
        auto& target = create_target(take, relation::write_kind::delete_, i1_, t1_, {0}, {});
        take.output() >> target.input();
        add_key_types(target, t::int4{});
        return {take, target};
    }
    std::pair<relation::step::take_flat&, relation::write&> create_upsert_take_target_i1() {
        auto& take = add_take(3);
        add_column_types(take, t::int4{}, t::float8{}, t::int8{});
        auto& target = create_target(take, relation::write_kind::insert_or_update, i1_, t1_, {0}, {1, 2});
        take.output() >> target.input();
        add_key_types(target, t::int4{});
        return {take, target};
    }
};

TEST_F(write_full_test, simple_insert) {
    auto&& [take, target] = create_insert_take_target_i1();
    create_processor_info();

    auto input = create_nullable_record<kind::int4, kind::float8, kind::int8>(0, 1.0, 2);
    variable_table_info input_variable_info{create_variable_table_info(destinations(take.columns()), input)};
    variable_table input_variables{input_variable_info};
    input_variables.store().set(input.ref());

    write_full op{
        0,
        *processor_info_,
        0,
        write_kind::insert,
        *i1_,
        target.keys(),
        target.columns(),
        &input_variable_info
    };

    auto tx = wrap(db_->create_transaction());
    auto mgr = sequence::manager{*db_};
    mock::task_context task_ctx{};
    write_full_context ctx{
        &task_ctx,
        input_variables,
        get_storage(*db_, i1_->simple_name()),
        tx.get(),
        &mgr,
        &resource_,
        &varlen_resource_
    };
    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ASSERT_EQ(status::ok, tx->commit(true));
    ASSERT_EQ(status::ok, tx->wait_for_commit(2000*1000*1000));
    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1_->simple_name(), create_record<kind::int4>(0), create_record<kind::float8, kind::int8>(0.0, 0), result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(create_record<kind::int4>(0), result[0].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(1.0, 2)), result[0].second);
}

TEST_F(write_full_test, simple_delete) {
    auto&& [take, target] = create_delete_take_target_i1();
    create_processor_info();

    auto input = create_nullable_record<kind::int4, kind::float8, kind::int8>(10, 0.0, 0);
    variable_table_info input_variable_info{create_variable_table_info(destinations(take.columns()), input)};
    variable_table input_variables{input_variable_info};
    input_variables.store().set(input.ref());

    write_full op{
        0,
        *processor_info_,
        0,
        write_kind::delete_,
        *i1_,
        target.keys(),
        target.columns(),
        &input_variable_info
    };

    std::this_thread::sleep_for(100ms);
    put( *db_, i1_->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db_, i1_->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));

    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1_->simple_name(), create_record<kind::int4>(), create_record<kind::float8, kind::int8>(), result);
    ASSERT_EQ(2, result.size());

    auto tx = wrap(db_->create_transaction());
    auto mgr = sequence::manager{*db_};
    mock::task_context task_ctx{};
    write_full_context ctx{
        &task_ctx,
        input_variables,
        get_storage(*db_, i1_->simple_name()),
        tx.get(),
        &mgr,
        &resource_,
        &varlen_resource_
    };

    ASSERT_TRUE(static_cast<bool>(op(ctx)));

    ASSERT_EQ(status::ok, tx->commit());
    wait_epochs();  // delete can be delayed
    result.clear();
    get(*db_, i1_->simple_name(), create_record<kind::int4>(), create_record<kind::float8, kind::int8>(), result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(create_record<kind::int4>(20), result[0].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(2.0, 200)), result[0].second);
}

TEST_F(write_full_test, upsert_as_insert) {
    auto&& [take, target] = create_upsert_take_target_i1();
    create_processor_info();

    auto input = create_nullable_record<kind::int4, kind::float8, kind::int8>(10, 0.0, 0);
    variable_table_info input_variable_info{create_variable_table_info(destinations(take.columns()), input)};
    variable_table input_variables{input_variable_info};
    input_variables.store().set(input.ref());

    write_full op{
        0,
        *processor_info_,
        0,
        write_kind::insert_or_update,
        *i1_,
        target.keys(),
        target.columns(),
        &input_variable_info
    };

    std::this_thread::sleep_for(100ms);
    put( *db_, i1_->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));

    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1_->simple_name(), create_record<kind::int4>(), create_record<kind::float8, kind::int8>(), result);
    ASSERT_EQ(1, result.size());

    auto tx = wrap(db_->create_transaction());
    auto mgr = sequence::manager{*db_};
    mock::task_context task_ctx{};
    write_full_context ctx{
        &task_ctx,
        input_variables,
        get_storage(*db_, i1_->simple_name()),
        tx.get(),
        &mgr,
        &resource_,
        &varlen_resource_
    };

    ASSERT_TRUE(static_cast<bool>(op(ctx)));

    ASSERT_EQ(status::ok, tx->commit());
    result.clear();
    get(*db_, i1_->simple_name(), create_record<kind::int4>(), create_record<kind::float8, kind::int8>(), result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ(create_record<kind::int4>(10), result[0].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(0.0, 0)), result[0].second);
    EXPECT_EQ(create_record<kind::int4>(20), result[1].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(2.0, 200)), result[1].second);
}

TEST_F(write_full_test, upsert_as_update) {
    auto&& [take, target] = create_upsert_take_target_i1();
    create_processor_info();

    auto input = create_nullable_record<kind::int4, kind::float8, kind::int8>(10, 0.0, 0);
    variable_table_info input_variable_info{create_variable_table_info(destinations(take.columns()), input)};
    variable_table input_variables{input_variable_info};
    input_variables.store().set(input.ref());

    write_full op{
        0,
        *processor_info_,
        0,
        write_kind::insert_or_update,
        *i1_,
        target.keys(),
        target.columns(),
        &input_variable_info
    };

    std::this_thread::sleep_for(100ms);
    put( *db_, i1_->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db_, i1_->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));

    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1_->simple_name(), create_record<kind::int4>(), create_record<kind::float8, kind::int8>(), result);
    ASSERT_EQ(2, result.size());

    auto tx = wrap(db_->create_transaction());
    auto mgr = sequence::manager{*db_};
    mock::task_context task_ctx{};
    write_full_context ctx{
        &task_ctx,
        input_variables,
        get_storage(*db_, i1_->simple_name()),
        tx.get(),
        &mgr,
        &resource_,
        &varlen_resource_
    };

    ASSERT_TRUE(static_cast<bool>(op(ctx)));

    ASSERT_EQ(status::ok, tx->commit());
    result.clear();
    get(*db_, i1_->simple_name(), create_record<kind::int4>(), create_record<kind::float8, kind::int8>(), result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ(create_record<kind::int4>(10), result[0].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(0.0, 0)), result[0].second);
    EXPECT_EQ(create_record<kind::int4>(20), result[1].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(2.0, 200)), result[1].second);
}

}

