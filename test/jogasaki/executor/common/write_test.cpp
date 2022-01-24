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
#include <jogasaki/executor/common/write.h>

#include <thread>

#include <gtest/gtest.h>

#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/analyzer/expression_resolution.h>
#include <yugawara/storage/column_value.h>
#include <yugawara/storage/table.h>

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

namespace jogasaki::executor::common {

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
using index = yugawara::storage::index;
using namespace yugawara::storage;
using namespace jogasaki::executor::process::impl::ops;
using namespace jogasaki::plan;

class write_test :
    public test_root,
    public kvs_test_base,
    public process::impl::ops::operator_test_utils {
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

    std::shared_ptr<table> t1nullable_ = create_table({
        "T1NULLABLE",
        {
            { "C0", t::int4(), nullity{true} },
            { "C1", t::float8(), nullity{true} },
            { "C2", t::int8(), nullity{true} },
        },
    });
    std::shared_ptr<index> i1nullable_ = create_primary_index(t1nullable_, {0}, {1,2});

    std::shared_ptr<table> t1_default_ = create_table({
        "T1_DEFAULT",
        {
            { "C0", t::int4(), nullity{true} },
            { "C1", t::float8(), nullity{true}, {column_value{v::float8{9}}} },
            { "C2", t::int8(), nullity{true}, {column_value{v::int8{99}}} },
        },
    });
    std::shared_ptr<index> i1_default_ = create_primary_index(t1_default_, {0}, {1,2});

    std::shared_ptr<yugawara::storage::sequence> seq_ = std::make_shared<yugawara::storage::sequence>(
        100,
        "SEQ"
    );
    std::shared_ptr<table> t1_seq_ = create_table({
        "T1_SEQ",
        {
            { "C0", t::int8(), nullity{true}, {column_value{seq_}} },
            { "C1", t::float8(), nullity{true} },
            { "C2", t::int8(), nullity{true}},
        },
    });
    std::shared_ptr<index> i1_seq_ = create_primary_index(t1_seq_, {0}, {1,2});
    template<class ...Args>
    std::shared_ptr<takatori::statement::write> create_write(
        std::shared_ptr<index> idx,
        std::shared_ptr<table> tbl,
        std::initializer_list<std::size_t> column_indices,
        std::initializer_list<std::initializer_list<takatori::util::rvalue_reference_wrapper<scalar::expression>>> tuples,
        Args...types
    ) {
        std::vector<std::reference_wrapper<takatori::type::data>> v{types...};
        std::vector<takatori::statement::write::tuple> tuple_list{};
        for(auto&& l : tuples) {
            tuple_list.emplace_back(l);
        }
        compiler_info_ = std::make_shared<yugawara::compiled_info>(expression_map_, variable_map_);
        std::vector<descriptor::variable> vars{};
        for(auto&& i : column_indices) {
            vars.emplace_back(bindings_(tbl->columns()[i]));
        }
        add_types(vars, types...);
        auto stmt = std::make_shared<takatori::statement::write>(
            takatori::statement::write::operator_kind_type::insert,
            bindings_(*idx),
            std::move(vars),
            std::move(tuple_list)
        );
        for(auto&& tup : stmt->tuples()) {
            for(std::size_t i=0, n=v.size(); i<n; ++i) {
                yugawara::analyzer::expression_resolution r{std::move(static_cast<takatori::type::data&>(v[i]))};
                expression_map_->bind(tup.elements()[i], r, true);
            }
        }
        return stmt;
    }
    std::shared_ptr<takatori::statement::write> create_write_i1(std::initializer_list<std::initializer_list<takatori::util::rvalue_reference_wrapper<scalar::expression>>> tuples) {
        return create_write(i1_, t1_, {0,1,2}, tuples, t::int4{}, t::float8{}, t::int8{});
    }

    std::shared_ptr<takatori::statement::write> create_write_i1nullable(std::initializer_list<std::initializer_list<takatori::util::rvalue_reference_wrapper<scalar::expression>>> tuples) {
        return create_write(i1nullable_, t1nullable_, {0,1,2}, tuples, t::int4{}, t::float8{}, t::int8{});
    }
    std::shared_ptr<takatori::statement::write> create_write_i1nullable_c0_only(std::initializer_list<std::initializer_list<takatori::util::rvalue_reference_wrapper<scalar::expression>>> tuples) {
        return create_write(i1nullable_, t1nullable_, {0}, tuples, t::int4{});
    }
};

inline scalar::immediate constant_i8(std::int64_t v) {
    return scalar::immediate { takatori::value::int8(v), takatori::type::int8{} };
}
inline scalar::immediate constant_text(std::string_view v) {
    return scalar::immediate { takatori::value::character(v), takatori::type::character(takatori::type::varying, 64) };
}
inline scalar::immediate constant_i4(int v) {
    return scalar::immediate { takatori::value::int4(v), takatori::type::int4{} };
}

inline scalar::immediate constant_f4(float v) {
    return scalar::immediate { takatori::value::float4(v), takatori::type::float4{} };
}

inline scalar::immediate constant_f8(double v) {
    return scalar::immediate { takatori::value::float8(v), takatori::type::float8{} };
}

TEST_F(write_test, simple_insert) {
    auto stmt = create_write_i1(
        {
            {
                constant_i4(10),
                constant_f8(1.0),
                constant_i8(100),
            }
        }
    );
    write wrt{
        write_kind::insert,
        *i1_,
        *stmt,
        resource_,
        *compiler_info_,
        nullptr
    };
    auto tx = std::shared_ptr{db_->create_transaction()};
    auto context = std::make_shared<request_context>(
        std::make_shared<configuration>(),
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        db_,
        std::make_shared<transaction_context>(tx),
        nullptr
    );
    ASSERT_TRUE(wrt(*context));
    ASSERT_EQ(status::ok, tx->commit(true));
    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1_->simple_name(), create_record<kind::int4>(0), create_record<kind::float8, kind::int8>(0.0, 0), result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(create_record<kind::int4>(10), result[0].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(1.0, 100)), result[0].second);
}

TEST_F(write_test, insert_two_records) {
    auto stmt = create_write_i1(
        {
            {
                constant_i4(20),
                constant_f8(2.0),
                constant_i8(200),
            },
            {
                constant_i4(10),
                constant_f8(1.0),
                constant_i8(100),
            }
        }
    );
    write wrt{
        write_kind::insert,
        *i1_,
        *stmt,
        resource_,
        *compiler_info_,
        nullptr
    };
    auto tx = std::shared_ptr{db_->create_transaction()};
    auto context = std::make_shared<request_context>(
        std::make_shared<configuration>(),
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        db_,
        std::make_shared<transaction_context>(tx),
        nullptr
    );
    ASSERT_TRUE(wrt(*context));
    ASSERT_EQ(status::ok, tx->commit(true));
    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1_->simple_name(), create_record<kind::int4>(0), create_record<kind::float8, kind::int8>(0.0, 0), result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ(create_record<kind::int4>(10), result[0].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(1.0, 100)), result[0].second);
    EXPECT_EQ(create_record<kind::int4>(20), result[1].first);
    EXPECT_EQ((create_record<kind::float8, kind::int8>(2.0, 200)), result[1].second);
}

TEST_F(write_test, nullable_table) {
    auto stmt = create_write_i1nullable(
        {
            {
                constant_i4(10),
                constant_f8(1.0),
                constant_i8(100),
            }
        }
    );
    write wrt{
        write_kind::insert,
        *i1nullable_,
        *stmt,
        resource_,
        *compiler_info_,
        nullptr
    };
    auto tx = std::shared_ptr{db_->create_transaction()};
    auto context = std::make_shared<request_context>(
        std::make_shared<configuration>(),
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        db_,
        std::make_shared<transaction_context>(tx),
        nullptr
    );
    ASSERT_TRUE(wrt(*context));
    ASSERT_EQ(status::ok, tx->commit(true));
    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1nullable_->simple_name(), create_nullable_record<kind::int4>(0), create_nullable_record<kind::float8, kind::int8>(0.0, 0), result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(create_nullable_record<kind::int4>(10), result[0].first);
    EXPECT_EQ((create_nullable_record<kind::float8, kind::int8>(1.0, 100)), result[0].second);
}

TEST_F(write_test, insert_nulls) {
    auto stmt = create_write_i1nullable_c0_only(
        {
            {
                constant_i4(10),
            }
        }
    );
    write wrt{
        write_kind::insert,
        *i1nullable_,
        *stmt,
        resource_,
        *compiler_info_,
        nullptr
    };
    auto tx = std::shared_ptr{db_->create_transaction()};
    auto context = std::make_shared<request_context>(
        std::make_shared<configuration>(),
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        db_,
        std::make_shared<transaction_context>(tx),
        nullptr
    );
    ASSERT_TRUE(wrt(*context));
    ASSERT_EQ(status::ok, tx->commit(true));
    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1nullable_->simple_name(), create_nullable_record<kind::int4>(0), create_nullable_record<kind::float8, kind::int8>(0.0, 0), result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(create_nullable_record<kind::int4>(10), result[0].first);
    EXPECT_EQ((create_nullable_record<kind::float8, kind::int8>({0.0, 0}, {true, true})), result[0].second);
}

TEST_F(write_test, insert_null_pkey) {
    auto stmt = create_write(i1nullable_, t1nullable_, {1, 2}, {
            {
                constant_f8(1.0),
                constant_i8(100),
            }
        },
        t::float8{},
        t::int8{}
    );
    write wrt{
        write_kind::insert,
        *i1nullable_,
        *stmt,
        resource_,
        *compiler_info_,
        nullptr
    };
    auto tx = std::shared_ptr{db_->create_transaction()};
    auto context = std::make_shared<request_context>(
        std::make_shared<configuration>(),
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        db_,
        std::make_shared<transaction_context>(tx),
        nullptr
    );
    ASSERT_TRUE(wrt(*context));
    ASSERT_EQ(status::ok, tx->commit(true));
    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1nullable_->simple_name(), create_nullable_record<kind::int4>(0), create_nullable_record<kind::float8, kind::int8>(0.0, 0), result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(create_nullable_record<kind::int4>({0}, {true}), result[0].first);
    EXPECT_EQ((create_nullable_record<kind::float8, kind::int8>(1.0, 100)), result[0].second);
}

TEST_F(write_test, default_value) {
    auto stmt = create_write(i1_default_, t1_default_, {0}, {
            {
                constant_i4(10),
            }
        },
        t::int4{}
    );
    write wrt{
        write_kind::insert,
        *i1_default_,
        *stmt,
        resource_,
        *compiler_info_,
        nullptr
    };

    auto tx = std::shared_ptr{db_->create_transaction()};
    auto context = std::make_shared<request_context>(
        std::make_shared<configuration>(),
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        db_,
        std::make_shared<transaction_context>(tx),
        nullptr
    );
    ASSERT_TRUE(wrt(*context));
    ASSERT_EQ(status::ok, tx->commit(true));
    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1_default_->simple_name(), create_nullable_record<kind::int4>(0), create_nullable_record<kind::float8, kind::int8>(0.0, 0), result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(create_nullable_record<kind::int4>(10), result[0].first);
    EXPECT_EQ((create_nullable_record<kind::float8, kind::int8>(9.0, 99)), result[0].second);
}

TEST_F(write_test, sequence_value) {
    tables_->add_sequence(seq_);
    auto stmt = create_write(i1_seq_, t1_seq_, {1,2}, {
            {
                constant_f8(1.0),
                constant_i8(100),
            },
            {
                constant_f8(2.0),
                constant_i8(200),
            },
            {
                constant_f8(3.0),
                constant_i8(300),
            },
        },
        t::float8{},
        t::int8{}
    );
    write wrt{
        write_kind::insert,
        *i1_seq_,
        *stmt,
        resource_,
        *compiler_info_,
        nullptr
    };

    auto mgr = std::make_unique<executor::sequence::manager>(*db_);
    mgr->register_sequences(tables_);
    auto tx = std::shared_ptr{db_->create_transaction()};
    auto context = std::make_shared<request_context>(
        std::make_shared<configuration>(),
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        db_,
        std::make_shared<transaction_context>(tx),
        mgr.get()
    );
    ASSERT_TRUE(wrt(*context));
    ASSERT_EQ(status::ok, tx->commit(true));
    std::vector<std::pair<basic_record, basic_record>> result{};
    get(*db_, i1_seq_->simple_name(), create_nullable_record<kind::int8>(0), create_nullable_record<kind::float8, kind::int8>(0.0, 0), result);
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::float8, kind::int8>(1.0, 100)), result[0].second);
    EXPECT_EQ((create_nullable_record<kind::float8, kind::int8>(2.0, 200)), result[1].second);
    EXPECT_EQ((create_nullable_record<kind::float8, kind::int8>(3.0, 300)), result[2].second);
    LOG(INFO) << result[0].first;
    LOG(INFO) << result[1].first;
    LOG(INFO) << result[2].first;
}

}

