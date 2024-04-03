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
#include <iostream>
#include <memory>
#include <optional>
#include <gtest/gtest.h>

#include <takatori/relation/sort_direction.h>
#include <takatori/type/character.h>
#include <takatori/type/date.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/varying.h>
#include <takatori/type/with_time_zone.h>
#include <takatori/util/string_builder.h>
#include <takatori/value/character.h>
#include <takatori/value/date.h>
#include <takatori/value/decimal.h>
#include <takatori/value/octet.h>
#include <takatori/value/primitive.h>
#include <takatori/value/time_of_day.h>
#include <takatori/value/time_point.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column_value.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index_feature.h>
#include <yugawara/storage/relation_kind.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/error_code.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/proto_debug_string.h>
#include <jogasaki/utils/storage_metadata_exception.h>
#include <jogasaki/utils/storage_metadata_serializer.h>

namespace jogasaki::utils {

using namespace yugawara;
namespace type = ::takatori::type;
using ::yugawara::variable::nullity;
using ::yugawara::storage::column_value;

class storage_metadata_serializer_test : public ::testing::Test {
public:

    storage::configurable_provider provider_{};

    yugawara::storage::index_feature_set index_features_{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
        ::yugawara::storage::index_feature::unique,
        ::yugawara::storage::index_feature::primary,
    };
    yugawara::storage::index_feature_set secondary_index_features_{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
    };

    void test_default_value_with_type(
        takatori::type::data&& type,
        takatori::value::data&& value
    );
};

std::string readable(std::string_view serialized) {
    proto::metadata::storage::IndexDefinition def{};
    if (!def.ParseFromArray(serialized.data(), serialized.size())) {
        []() {
            FAIL();
        }();
    }
    return utils::to_debug_string(def);
}

TEST_F(storage_metadata_serializer_test, simple) {
    std::shared_ptr<::yugawara::storage::table> t = provider_.add_table({
        "TT",
        {
            { "C0", type::int8(), nullity{false} },
            { "C1", type::character(type::varying), nullity{true} },
        },
    });
    auto primary = provider_.add_index({
        t,
        t->simple_name(),
        {
            t->columns()[0],
        },
        {
            t->columns()[1],
        },
        index_features_
    });

    auto secondary = provider_.add_index({
        t,
        "TT_SECONDARY",
        {
            t->columns()[1],
        },
        {},
        secondary_index_features_
    });

    storage_metadata_serializer ser{};
    std::string out{};
    ASSERT_NO_THROW(ser.serialize(*primary, out));
    std::string sec{};
    ASSERT_NO_THROW(ser.serialize(*secondary, sec));
    std::cerr << "sec : " << readable(sec) << std::endl;

    auto deserialized = std::make_shared<storage::configurable_provider>();
    ASSERT_NO_THROW(ser.deserialize(out, provider_, *deserialized));
    auto i = deserialized->find_index("TT");
    ASSERT_TRUE(i);
    auto deserialized2 = std::make_shared<storage::configurable_provider>();
    ASSERT_NO_THROW(ser.deserialize(sec, provider_, *deserialized2));
    auto i2 = deserialized2->find_index("TT_SECONDARY");
    ASSERT_TRUE(i2);
    std::string out2{};
    ASSERT_NO_THROW(ser.serialize(*i, out2));
    EXPECT_EQ(readable(out2), readable(out));
    std::string sec2{};
    ASSERT_NO_THROW(ser.serialize(*secondary, sec2));
    EXPECT_EQ(readable(sec2), readable(sec));
}

void test_index(
    yugawara::storage::index& primary,
    yugawara::storage::configurable_provider const& provider,
    storage::configurable_provider& deserialized
) {
    storage_metadata_serializer ser{};
    std::string out{};
    ASSERT_NO_THROW(ser.serialize(primary, out));
    std::cerr << "out : " << readable(out) << std::endl;
    ASSERT_NO_THROW(ser.deserialize(out, provider, deserialized));
    auto i = deserialized.find_index(primary.simple_name());
    ASSERT_TRUE(i);
    std::string out2{};
    ASSERT_NO_THROW(ser.serialize(*i, out2));
    EXPECT_EQ(readable(out2), readable(out));
}

template <class T>
std::string to_string(T const& t) {
    return takatori::util::string_builder{} << t << takatori::util::string_builder::to_string;
}

TEST_F(storage_metadata_serializer_test, primary_index_with_types) {
    std::shared_ptr<::yugawara::storage::table> t = provider_.add_table({
        "TT",
        {
            { "i1", type::int1(), nullity{false} },
            { "i2", type::int2(), nullity{true} },
            { "i4", type::int4(), nullity{true} },
            { "i8", type::int8(), nullity{true} },
            { "f4", type::float4(), nullity{true} },
            { "f8", type::float8(), nullity{true} },
            { "ch_5", type::character(~type::varying, 5), nullity{true} },
            { "vc_10", type::character(type::varying, 10), nullity{true} },
            { "vc_a", type::character(type::varying), nullity{true} },
            { "oc", type::octet(~type::varying), nullity{true} },
            { "ov", type::octet(type::varying), nullity{true} },
            { "dec_5_3", type::decimal(5, 3), nullity{true} },
            { "dec_a_3", type::decimal(std::nullopt, 5), nullity{true} },
            { "dt", type::date(), nullity{true} },
            { "tod", type::time_of_day(~takatori::type::with_time_zone), nullity{true} },
            { "todtz", type::time_of_day(takatori::type::with_time_zone), nullity{true} },
            { "todtz", type::time_of_day(takatori::type::with_time_zone), nullity{true} },
            { "tp", type::time_point(~takatori::type::with_time_zone), nullity{true} },
            { "tptz", type::time_point(takatori::type::with_time_zone), nullity{true} },
        },
    });
    auto primary = provider_.add_index({
        t,
        t->simple_name(),
        {
            t->columns()[0],
        },
        {
            t->columns()[1],
            t->columns()[2],
            t->columns()[3],
            t->columns()[4],
            t->columns()[5],
            t->columns()[6],
            t->columns()[7],
            t->columns()[8],
            t->columns()[9],
            t->columns()[10],
            t->columns()[11],
            t->columns()[12],
            t->columns()[13],
            t->columns()[14],
            t->columns()[15],
            t->columns()[16],
            t->columns()[17],
        },
        index_features_
    });

    auto deserialized = std::make_shared<storage::configurable_provider>();
    test_index(*primary, provider_, *deserialized);
    auto t2 = deserialized->find_table("TT");
    EXPECT_EQ(to_string(*t2), to_string(*t));
}

TEST_F(storage_metadata_serializer_test, secondary_index) {
    std::shared_ptr<::yugawara::storage::table> t = provider_.add_table({
        "TT",
        {
            { "C0", type::int8(), nullity{false} },
            { "C1", type::character(type::varying), nullity{true} },
        },
    });
    auto primary = provider_.add_index({
        t,
        t->simple_name(),
        {
            t->columns()[0],
        },
        {
            t->columns()[1],
        },
        index_features_
    });

    auto secondary = provider_.add_index({
        t,
        "TT_SECONDARY",
        {
            {t->columns()[1], takatori::relation::sort_direction::descendant},
            {t->columns()[0], takatori::relation::sort_direction::ascendant},
        },
        {},
        secondary_index_features_
    });

    auto deserialized = std::make_shared<storage::configurable_provider>();
    test_index(*secondary, provider_, *deserialized);
    auto i2 = deserialized->find_index("TT_SECONDARY");
    EXPECT_EQ(to_string(*i2), to_string(*secondary));
}

TEST_F(storage_metadata_serializer_test, default_value) {
    std::shared_ptr<::yugawara::storage::table> t = provider_.add_table({
        "TT",
        {
            { "C0", type::int8(), nullity{false} },
            { "C1", type::int8(), nullity{true}, column_value{std::make_shared<takatori::value::int8 const>(100)} },
        },
    });
    auto primary = provider_.add_index({
        t,
        t->simple_name(),
        {
            t->columns()[0],
        },
        {
            t->columns()[1],
        },
        index_features_
    });
    auto deserialized = std::make_shared<storage::configurable_provider>();
    test_index(*primary, provider_, *deserialized);
    auto t2 = deserialized->find_table("TT");
    EXPECT_EQ(to_string(*t2), to_string(*t));
}

TEST_F(storage_metadata_serializer_test, default_value_with_types) {
    std::shared_ptr<::yugawara::storage::table> t = provider_.add_table({
        "TT",
        {
            { "i1", type::int1(), nullity{false}, column_value{std::make_shared<takatori::value::int4 const>(100)}},
            { "i2", type::int2(), nullity{true}, column_value{std::make_shared<takatori::value::int4 const>(100)} },
            { "i4", type::int4(), nullity{true}, column_value{std::make_shared<takatori::value::int4 const>(100)} },
            { "i8", type::int8(), nullity{true}, column_value{std::make_shared<takatori::value::int8 const>(100)} },
            { "f4", type::float4(), nullity{true}, column_value{std::make_shared<takatori::value::float4 const>(100)} },
            { "f8", type::float8(), nullity{true}, column_value{std::make_shared<takatori::value::float8 const>(100)} },
            { "ch_5", type::character(~type::varying, 5), nullity{true}, column_value{std::make_shared<takatori::value::character const>(std::to_string(100))} },
            { "vc_10", type::character(type::varying, 10), nullity{true}, column_value{std::make_shared<takatori::value::character const>(std::to_string(100))} },
            { "vc_a", type::character(type::varying), nullity{true}, column_value{std::make_shared<takatori::value::character const>(std::to_string(100))} },
            { "oc", type::octet(~type::varying), nullity{true}, column_value{std::make_shared<takatori::value::octet const>(std::to_string(100))} },
            { "ov", type::octet(type::varying), nullity{true}, column_value{std::make_shared<takatori::value::octet const>(std::to_string(100))} },
            { "dec_5_3", type::decimal(5, 3), nullity{true}, column_value{std::make_shared<takatori::value::decimal const>("100")} },
            { "dec_a_3", type::decimal(std::nullopt, 5), nullity{true}, column_value{std::make_shared<takatori::value::decimal const>("100")} },
            { "dt", type::date(), nullity{true}, column_value{std::make_shared<takatori::value::date const>(2000, 1, 1)} },
            { "tod", type::time_of_day(~takatori::type::with_time_zone), nullity{true}, column_value{std::make_shared<takatori::value::time_of_day const>(12, 0, 0)} },
            { "todtz", type::time_of_day(takatori::type::with_time_zone), nullity{true}, column_value{std::make_shared<takatori::value::time_of_day const>(12, 0, 0)} },
            { "tp", type::time_point(~takatori::type::with_time_zone), nullity{true}, column_value{std::make_shared<takatori::value::time_point const>(2000, 1, 1, 12, 0, 0)} },
            { "tptz", type::time_point(takatori::type::with_time_zone), nullity{true}, column_value{std::make_shared<takatori::value::time_point const>(2000, 1, 1, 12, 0, 0)} },
        },
    });
    auto primary = provider_.add_index({
        t,
        t->simple_name(),
        {
            t->columns()[0],
        },
        {
            t->columns()[1],
            t->columns()[2],
            t->columns()[3],
            t->columns()[4],
            t->columns()[5],
            t->columns()[6],
            t->columns()[7],
            t->columns()[8],
            t->columns()[9],
            t->columns()[10],
            t->columns()[11],
            t->columns()[12],
            t->columns()[13],
            t->columns()[14],
            t->columns()[15],
            t->columns()[16],
            t->columns()[17],
        },
        index_features_
    });
    auto deserialized = std::make_shared<storage::configurable_provider>();
    test_index(*primary, provider_, *deserialized);
    auto t2 = deserialized->find_table("TT");
    EXPECT_EQ(to_string(*t2), to_string(*t));
}

TEST_F(storage_metadata_serializer_test, default_value_sequence) {
    auto s0 = std::make_shared<storage::sequence>(
        1000,
        "seq0"
    );
    auto s1 = std::make_shared<storage::sequence>(
        1000,
        "seq1",
        10,
        100,
        1000,
        10000,
        false
    );
    provider_.add_sequence(s0);
    provider_.add_sequence(s1);
    std::shared_ptr<::yugawara::storage::table> t = provider_.add_table({
        "TT",
        {
            { "C0", type::int8(), nullity{false}, {s0} },
            { "C1", type::int8(), nullity{true}, {s1} },
        },
    });
    auto primary = provider_.add_index({
        t,
        t->simple_name(),
        {
            t->columns()[0],
        },
        {
            t->columns()[1],
        },
        index_features_
    });

    auto deserialized = std::make_shared<storage::configurable_provider>();
    test_index(*primary, provider_, *deserialized);
    auto t2 = deserialized->find_table("TT");
    EXPECT_EQ(to_string(*t2), to_string(*t));
    auto seq0 = deserialized->find_sequence("seq0");
    ASSERT_TRUE(seq0);
    auto seq1 = deserialized->find_sequence("seq1");
    ASSERT_TRUE(seq1);
    EXPECT_EQ(to_string(*s0), to_string(*seq0));
    EXPECT_EQ(to_string(*s1), to_string(*seq1));
}

TEST_F(storage_metadata_serializer_test, synthesized_flag) {
    std::shared_ptr<::yugawara::storage::table> t = provider_.add_table({
        "TT",
        {
            { "C0", type::int8(), nullity{false} },
            { "C1", type::character(type::varying), nullity{true} },
        },
    });
    auto primary = provider_.add_index({
        t,
        t->simple_name(),
        {
            t->columns()[0],
        },
        {
            t->columns()[1],
        },
        index_features_
    });

    auto secondary = provider_.add_index({
        t,
        "TT_SECONDARY",
        {
            t->columns()[1],
        },
        {},
        secondary_index_features_
    });

    storage_metadata_serializer ser{};
    {
        proto::metadata::storage::IndexDefinition idef{};
        ASSERT_NO_THROW(ser.serialize(*primary, idef, metadata_serializer_option{true}));
        ASSERT_TRUE(idef.synthesized());
    }
    {
        proto::metadata::storage::IndexDefinition idef{};
        ASSERT_NO_THROW(ser.serialize(*primary, idef, metadata_serializer_option{false}));
        ASSERT_FALSE(idef.synthesized());
    }
}

void storage_metadata_serializer_test::test_default_value_with_type(
    takatori::type::data&& type,
    takatori::value::data&& value
) {
    storage::configurable_provider provider{};
    std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
        "TT",
        {
            { "C0", std::move(type), nullity{true}, column_value{std::move(value)} },
        },
    });
    auto primary = provider.add_index({
        t,
        t->simple_name(),
        {
            t->columns()[0],
        },
        {},
        index_features_
    });
    auto deserialized = std::make_shared<storage::configurable_provider>();
    test_index(*primary, provider, *deserialized);
    auto t2 = deserialized->find_table("TT");
    EXPECT_EQ(to_string(*t2), to_string(*t));
}

TEST_F(storage_metadata_serializer_test, varieties_of_default_value_types) {
    // verify default values rounds trip even if default its type is different from column type
    test_default_value_with_type(takatori::type::int4(), takatori::value::int4{-11});
    test_default_value_with_type(takatori::type::int4(), takatori::value::int8{-11});
    test_default_value_with_type(takatori::type::int4(), takatori::value::float4{-11.0F});
    test_default_value_with_type(takatori::type::int4(), takatori::value::float8{-11.0});
    test_default_value_with_type(takatori::type::int4(), takatori::value::decimal{-11});

    test_default_value_with_type(takatori::type::int8(), takatori::value::int4{-11});
    test_default_value_with_type(takatori::type::int8(), takatori::value::int8{-11});
    test_default_value_with_type(takatori::type::int8(), takatori::value::float4{-11.0F});
    test_default_value_with_type(takatori::type::int8(), takatori::value::float8{-11.0});
    test_default_value_with_type(takatori::type::int8(), takatori::value::decimal{-11});

    test_default_value_with_type(takatori::type::float4(), takatori::value::int4{-11});
    test_default_value_with_type(takatori::type::float4(), takatori::value::int8{-11});
    test_default_value_with_type(takatori::type::float4(), takatori::value::float4{-11.0F});
    test_default_value_with_type(takatori::type::float4(), takatori::value::float8{-11.0});
    test_default_value_with_type(takatori::type::float4(), takatori::value::decimal{-11});

    test_default_value_with_type(takatori::type::float8(), takatori::value::int4{-11});
    test_default_value_with_type(takatori::type::float8(), takatori::value::int8{-11});
    test_default_value_with_type(takatori::type::float8(), takatori::value::float4{-11.0F});
    test_default_value_with_type(takatori::type::float8(), takatori::value::float8{-11.0});
    test_default_value_with_type(takatori::type::float8(), takatori::value::decimal{-11});

    test_default_value_with_type(takatori::type::decimal(), takatori::value::int4{-11});
    test_default_value_with_type(takatori::type::decimal(), takatori::value::int8{-11});
    test_default_value_with_type(takatori::type::decimal(), takatori::value::float4{-11.0F});
    test_default_value_with_type(takatori::type::decimal(), takatori::value::float8{-11.0});
    test_default_value_with_type(takatori::type::decimal(), takatori::value::decimal{-11});
}

TEST_F(storage_metadata_serializer_test, already_exists_error) {
    std::shared_ptr<::yugawara::storage::table> t = provider_.add_table({
        "TT",
        {
            { "C0", type::int8(), nullity{false} },
        },
    });
    auto primary = provider_.add_index({
        t,
        t->simple_name(),
        {
            t->columns()[0],
        },
        {},
        index_features_
    });

    storage_metadata_serializer ser{};
    proto::metadata::storage::IndexDefinition idef{};
    ASSERT_NO_THROW(ser.serialize(*primary, idef, metadata_serializer_option{true}));

    auto deserialized = std::make_shared<storage::configurable_provider>();
    t = deserialized->add_table({
        "TT",
        {
            { "C0", type::int8(), nullity{false} },
        },
    });
    bool caught = false;
    try {
        (ser.deserialize(idef, provider_, *deserialized));
    } catch (storage_metadata_exception const& e) {
        caught = true;
        EXPECT_EQ(status::err_already_exists, e.get_status());
        EXPECT_EQ(error_code::target_already_exists_exception, e.get_code());
        EXPECT_EQ("table \"TT\" already exists", std::string{e.what()});
    }
    ASSERT_TRUE(caught);
    auto i = deserialized->find_index("TT");
}


}
