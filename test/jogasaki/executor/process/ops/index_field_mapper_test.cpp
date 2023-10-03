/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <jogasaki/executor/process/impl/ops/index_field_mapper.h>

#include <string>
#include <gtest/gtest.h>

#include <jogasaki/test_root.h>
#include <jogasaki/executor/process/impl/ops/find_context.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/kvs_test_base.h>

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace jogasaki::mock;
using namespace jogasaki::kvs;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class index_field_mapper_test :
    public test_root,
    public kvs_test_base {
public:
    void SetUp() override {
        kvs_db_setup();
    }
    void TearDown() override {
        kvs_db_teardown();
    }
};

using k = meta::field_type_kind;

TEST_F(index_field_mapper_test, simple) {
    auto t1 = db_->create_storage("T1");
    auto i2 = db_->create_storage("I2");
    memory::page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    {
        {
            // secondary index(i2) int4,int4 -> int8
            // primary index(t1)   int8, int4
            std::string src(100, 0);
            std::string tgt_k(100, 0);
            std::string tgt_v(100, 0);
            kvs::writable_stream s{src};
            kvs::writable_stream t_k{tgt_k};
            kvs::writable_stream t_v{tgt_v};

            basic_record secondary_rec{create_nullable_record<k::int4, k::int4, k::int8>(1, 1, 10)};
            auto secondary_rec_meta = secondary_rec.record_meta();
            encode_nullable(secondary_rec.ref(), secondary_rec_meta->value_offset(0), secondary_rec_meta->nullity_offset(0), secondary_rec_meta->at(0), spec_asc, s);
            encode_nullable(secondary_rec.ref(), secondary_rec_meta->value_offset(1), secondary_rec_meta->nullity_offset(1), secondary_rec_meta->at(1), spec_asc, s);
            encode_nullable(secondary_rec.ref(), secondary_rec_meta->value_offset(2), secondary_rec_meta->nullity_offset(2), secondary_rec_meta->at(2), spec_asc, s);

            basic_record primary_rec{create_nullable_record<k::int8, k::int4>(10, 100)};
            auto primary_rec_meta = primary_rec.record_meta();
            encode_nullable(primary_rec.ref(), primary_rec_meta->value_offset(0), primary_rec_meta->nullity_offset(0), primary_rec_meta->at(0), spec_asc, t_k);
            encode_nullable(primary_rec.ref(), primary_rec_meta->value_offset(1), primary_rec_meta->nullity_offset(1), primary_rec_meta->at(1), spec_asc, t_v);

            auto tx = db_->create_transaction();
            ASSERT_EQ(status::ok, i2->put(*tx, {s.data(), s.size()}, ""));
            ASSERT_EQ(status::ok, t1->put(*tx, {t_k.data(), t_k.size()}, {t_v.data(), t_v.size()}));
            ASSERT_EQ(status::ok, tx->commit());
        }
        {
            basic_record result{create_nullable_record<k::int8, k::int4>(0, 0)};
            index_field_mapper mapper{
                {
                    {
                        meta::field_type{field_enum_tag_t<k::int8>{}},
                        true,
                        result.record_meta()->value_offset(0),
                        result.record_meta()->nullity_offset(0),
                        true,
                        kvs::spec_key_ascending
                    },
                },
                {
                    {
                        meta::field_type{field_enum_tag_t<k::int4>{}},
                        true,
                        result.record_meta()->value_offset(1),
                        result.record_meta()->nullity_offset(1),
                        true,
                        kvs::spec_key_ascending
                    },
                },
                {
                    {
                        meta::field_type{field_enum_tag_t<k::int4>{}},
                        true,
                        kvs::spec_key_ascending
                    },
                    {
                        meta::field_type{field_enum_tag_t<k::int4>{}},
                        true,
                        kvs::spec_key_ascending
                    },
                },
            };
            {
                auto tx = wrap(db_->create_transaction());
                std::unique_ptr<iterator> it{};
                ASSERT_EQ(status::ok, i2->scan(*tx, "", end_point_kind::unbound, "", end_point_kind::unbound, it));
                ASSERT_EQ(status::ok, it->next());

                std::string_view key{};
                std::string_view value{};
                ASSERT_EQ(status::ok, it->key(key));
                ASSERT_EQ(status::ok, it->value(value));
                ASSERT_EQ(status::ok, mapper(key, value, result.ref(), *t1, *tx, &resource));
                it.reset();
                ASSERT_EQ(status::ok, tx->commit());
                ASSERT_EQ(10, result.ref().get_value<std::int64_t>(result.record_meta()->value_offset(0)));
                ASSERT_EQ(100, result.ref().get_value<std::int32_t>(result.record_meta()->value_offset(1)));
            }
        }
    }
}

TEST_F(index_field_mapper_test, without_secondary) {
    auto t1 = db_->create_storage("T1");
    memory::page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    {
        {
            // primary index(t1)   int8, int4
            std::string tgt_k(100, 0);
            std::string tgt_v(100, 0);
            kvs::writable_stream t_k{tgt_k};
            kvs::writable_stream t_v{tgt_v};

            basic_record primary_rec{create_nullable_record<k::int8, k::int4>(10, 100)};
            auto primary_rec_meta = primary_rec.record_meta();
            encode_nullable(primary_rec.ref(), primary_rec_meta->value_offset(0), primary_rec_meta->nullity_offset(0), primary_rec_meta->at(0), spec_asc, t_k);
            encode_nullable(primary_rec.ref(), primary_rec_meta->value_offset(1), primary_rec_meta->nullity_offset(1), primary_rec_meta->at(1), spec_asc, t_v);

            auto tx = db_->create_transaction();
            ASSERT_EQ(status::ok, t1->put(*tx, {t_k.data(), t_k.size()}, {t_v.data(), t_v.size()}));
            ASSERT_EQ(status::ok, tx->commit());
        }
        {
            basic_record result{create_nullable_record<k::int8, k::int4>(0, 0)};
            index_field_mapper mapper{
                {
                    {
                        meta::field_type{field_enum_tag_t<k::int8>{}},
                        true,
                        result.record_meta()->value_offset(0),
                        result.record_meta()->nullity_offset(0),
                        true,
                        kvs::spec_key_ascending
                    },
                },
                {
                    {
                        meta::field_type{field_enum_tag_t<k::int4>{}},
                        true,
                        result.record_meta()->value_offset(1),
                        result.record_meta()->nullity_offset(1),
                        true,
                        kvs::spec_key_ascending
                    },
                }
            };
            {
                auto tx = wrap(db_->create_transaction());
                std::unique_ptr<iterator> it{};
                ASSERT_EQ(status::ok, t1->scan(*tx, "", end_point_kind::unbound, "", end_point_kind::unbound, it));
                ASSERT_EQ(status::ok, it->next());

                std::string_view key{};
                std::string_view value{};
                ASSERT_EQ(status::ok, it->key(key));
                ASSERT_EQ(status::ok, it->value(value));
                ASSERT_EQ(status::ok, mapper(key, value, result.ref(), *t1, *tx, &resource));
                it.reset();
                ASSERT_EQ(status::ok, tx->commit());
                ASSERT_EQ(10, result.ref().get_value<std::int64_t>(result.record_meta()->value_offset(0)));
                ASSERT_EQ(100, result.ref().get_value<std::int32_t>(result.record_meta()->value_offset(1)));
            }
        }
    }
}

}

