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
#include <jogasaki/kvs/coder.h>

#include <string>

#include <takatori/util/object_creator.h>
#include <gtest/gtest.h>

#include <jogasaki/test_root.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/executor/process/impl/expression/any.h>
#include <jogasaki/kvs/environment.h>

#include <jogasaki/mock_memory_resource.h>

namespace jogasaki::kvs {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;
using namespace kvs::details;

class coder_test : public test_root {
public:
    using kind = meta::field_type_kind;
    static void SetUpTestCase() {}
    coder_test() {}
private:
};

constexpr kvs::order asc = kvs::order::ascending;
constexpr kvs::order desc = kvs::order::descending;

TEST_F(coder_test, simple) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    int_t<32> i32{1};
    int_t<64> i64{2};
    float_t<32> f32{3};
    float_t<64> f64{4};
    mock_memory_resource resource{};
    accessor::text txt{&resource, "ABC"sv};
    accessor::text txt2{&resource, "ABC"sv};
    s.write(i32, asc);
    s.write(f32, asc);
    s.write(i64, asc);
    s.write(f64, asc);
    s.write(txt, asc);

    s.reset();

    ASSERT_EQ(i32, s.read<std::int32_t>(asc, false));
    ASSERT_EQ(f32, s.read<float>(asc, false));
    ASSERT_EQ(i64, s.read<std::int64_t>(asc, false));
    ASSERT_EQ(f64, s.read<double>(asc, false));
    ASSERT_EQ(txt2, s.read<accessor::text>(asc, false, &resource));
}

TEST_F(coder_test, descendant) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    int_t<32> i32{1};
    int_t<64> i64{2};
    float_t<32> f32{3};
    float_t<64> f64{4};
    mock_memory_resource resource{};
    accessor::text txt{&resource, "ABC"sv};
    accessor::text txt2{&resource, "ABC"sv};
    s.write(i32, desc);
    s.write(f32, desc);
    s.write(i64, desc);
    s.write(f64, desc);
    s.write(txt, desc);

    s.reset();

    ASSERT_EQ(i32, s.read<std::int32_t>(desc, false));
    ASSERT_EQ(f32, s.read<float>(desc, false));
    ASSERT_EQ(i64, s.read<std::int64_t>(desc, false));
    ASSERT_EQ(f64, s.read<double>(desc, false));
    ASSERT_EQ(txt, s.read<accessor::text>(desc, false, &resource));
}

TEST_F(coder_test, i32_asc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    int_t<32> i1{2};
    int_t<32> i2{-2};
    s.write(i1, asc);
    s.write(i2, asc);

    s.reset();

    ASSERT_EQ(i1, s.read<std::int32_t>(asc, false));
    ASSERT_EQ(i2, s.read<std::int32_t>(asc, false));
    EXPECT_EQ('\x80', buf[0]);
    EXPECT_EQ('\x00', buf[1]);
    EXPECT_EQ('\x00', buf[2]);
    EXPECT_EQ('\x02', buf[3]);
    EXPECT_EQ('\x7F', buf[4]);
    EXPECT_EQ('\xFF', buf[5]);
    EXPECT_EQ('\xFF', buf[6]);
    EXPECT_EQ('\xFE', buf[7]);
}

TEST_F(coder_test, i32_desc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    int_t<32> i1{2};
    int_t<32> i2{-2};
    s.write(i1, desc);
    s.write(i2, desc);

    s.reset();

    ASSERT_EQ(i1, s.read<std::int32_t>(desc, false));
    ASSERT_EQ(i2, s.read<std::int32_t>(desc, false));
    EXPECT_EQ('\x7F', buf[0]);
    EXPECT_EQ('\xFF', buf[1]);
    EXPECT_EQ('\xFF', buf[2]);
    EXPECT_EQ('\xFD', buf[3]);
    EXPECT_EQ('\x80', buf[4]);
    EXPECT_EQ('\x00', buf[5]);
    EXPECT_EQ('\x00', buf[6]);
    EXPECT_EQ('\x01', buf[7]);
}

TEST_F(coder_test, i64_asc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    int_t<64> i1{2};
    int_t<64> i2{-2};
    s.write(i1, asc);
    s.write(i2, asc);

    s.reset();

    ASSERT_EQ(i1, s.read<std::int64_t>(asc, false));
    ASSERT_EQ(i2, s.read<std::int64_t>(asc, false));
    EXPECT_EQ('\x80', buf[0]);
    EXPECT_EQ('\x00', buf[1]);
    EXPECT_EQ('\x00', buf[2]);
    EXPECT_EQ('\x00', buf[3]);
    EXPECT_EQ('\x00', buf[4]);
    EXPECT_EQ('\x00', buf[5]);
    EXPECT_EQ('\x00', buf[6]);
    EXPECT_EQ('\x02', buf[7]);

    EXPECT_EQ('\x7F', buf[8]);
    EXPECT_EQ('\xFF', buf[9]);
    EXPECT_EQ('\xFF', buf[10]);
    EXPECT_EQ('\xFF', buf[11]);
    EXPECT_EQ('\xFF', buf[12]);
    EXPECT_EQ('\xFF', buf[13]);
    EXPECT_EQ('\xFF', buf[14]);
    EXPECT_EQ('\xFE', buf[15]);
}

TEST_F(coder_test, i16_asc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    int_t<16> i1{2};
    int_t<16> i2{-2};
    s.write(i1, asc);
    s.write(i2, asc);

    s.reset();

    ASSERT_EQ(i1, s.read<std::int16_t>(asc, false));
    ASSERT_EQ(i2, s.read<std::int16_t>(asc, false));
    EXPECT_EQ('\x80', buf[0]);
    EXPECT_EQ('\x02', buf[1]);
    EXPECT_EQ('\x7F', buf[2]);
    EXPECT_EQ('\xFE', buf[3]);
}

TEST_F(coder_test, i8_asc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    int_t<8> i1{2};
    int_t<8> i2{-2};
    s.write(i1, asc);
    s.write(i2, asc);

    s.reset();

    ASSERT_EQ(i1, s.read<std::int8_t>(asc, false));
    ASSERT_EQ(i2, s.read<std::int8_t>(asc, false));
    EXPECT_EQ('\x82', buf[0]);
    EXPECT_EQ('\x7E', buf[1]);
}

TEST_F(coder_test, f32_asc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    float_t<32> f1{2};
    float_t<32> f2{-2};
    s.write(f1, asc);
    s.write(f2, asc);

    s.reset();

    ASSERT_EQ(f1, s.read<float>(asc, false));
    ASSERT_EQ(f2, s.read<float>(asc, false));
    EXPECT_EQ('\xC0', buf[0]);
    EXPECT_EQ('\x00', buf[1]);
    EXPECT_EQ('\x00', buf[2]);
    EXPECT_EQ('\x00', buf[3]);
    EXPECT_EQ('\x3F', buf[4]);
    EXPECT_EQ('\xFF', buf[5]);
    EXPECT_EQ('\xFF', buf[6]);
    EXPECT_EQ('\xFF', buf[7]);
}

TEST_F(coder_test, f32_desc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    float_t<32> f1{2};
    float_t<32> f2{-2};
    s.write(f1, desc);
    s.write(f2, desc);

    s.reset();

    ASSERT_EQ(f1, s.read<float>(desc, false));
    ASSERT_EQ(f2, s.read<float>(desc, false));
    EXPECT_EQ('\x3F', buf[0]);
    EXPECT_EQ('\xFF', buf[1]);
    EXPECT_EQ('\xFF', buf[2]);
    EXPECT_EQ('\xFF', buf[3]);
    EXPECT_EQ('\xC0', buf[4]);
    EXPECT_EQ('\x00', buf[5]);
    EXPECT_EQ('\x00', buf[6]);
    EXPECT_EQ('\x00', buf[7]);
}

TEST_F(coder_test, float_nan) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    float_t<32> f1{std::nanf("1")};
    float_t<32> f2{std::nanf("2")};
    float_t<64> f3{std::nan("3")};
    float_t<64> f4{std::nan("4")};
    s.write(f1, asc);
    s.write(f2, desc);
    s.write(f3, asc);
    s.write(f4, desc);

    s.reset();

    ASSERT_TRUE(std::isnan(s.read<float>(asc, false)));
    ASSERT_TRUE(std::isnan(s.read<float>(desc, false)));
    ASSERT_TRUE(std::isnan(s.read<double>(asc, false)));
    ASSERT_TRUE(std::isnan(s.read<double>(desc, false)));
}

TEST_F(coder_test, f64_asc) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    float_t<64> f1{2};
    float_t<64> f2{-2};
    s.write(f1, asc);
    s.write(f2, asc);

    s.reset();

    ASSERT_EQ(f1, s.read<double>(asc, false));
    ASSERT_EQ(f2, s.read<double>(asc, false));
    EXPECT_EQ('\xC0', buf[0]);
    EXPECT_EQ('\x00', buf[1]);
    EXPECT_EQ('\x00', buf[2]);
    EXPECT_EQ('\x00', buf[3]);
    EXPECT_EQ('\x00', buf[4]);
    EXPECT_EQ('\x00', buf[5]);
    EXPECT_EQ('\x00', buf[6]);
    EXPECT_EQ('\x00', buf[7]);
    EXPECT_EQ('\x3F', buf[8]);
    EXPECT_EQ('\xFF', buf[9]);
    EXPECT_EQ('\xFF', buf[10]);
    EXPECT_EQ('\xFF', buf[11]);
    EXPECT_EQ('\xFF', buf[12]);
    EXPECT_EQ('\xFF', buf[13]);
    EXPECT_EQ('\xFF', buf[14]);
    EXPECT_EQ('\xFF', buf[15]);
}

TEST_F(coder_test, text) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    mock_memory_resource resource{};
    accessor::text txt{&resource, "ABC"sv};
    s.write(txt, asc);

    s.reset();

    ASSERT_EQ(txt, s.read<accessor::text>(asc, false, &resource));
    EXPECT_EQ('\x80', buf[0]);
    EXPECT_EQ('\x03', buf[1]);
    EXPECT_EQ('A', buf[2]);
    EXPECT_EQ('B', buf[3]);
    EXPECT_EQ('C', buf[4]);
}

TEST_F(coder_test, empty_text) {
    std::string buf(100, 0);
    kvs::stream s{buf};
    mock_memory_resource resource{};
    accessor::text txt{&resource, ""sv};
    s.write(txt, asc);

    s.reset();
    auto result = s.read<accessor::text>(asc, false, &resource);
    ASSERT_EQ(txt, result);
    ASSERT_EQ(0, result.size());
    EXPECT_EQ('\x80', buf[0]);
    EXPECT_EQ('\x00', buf[1]);
}

TEST_F(coder_test, encode_decode) {
    std::string src(100, 0);
    std::string tgt(100, 0);
    kvs::stream s{src};
    kvs::stream t{tgt};

    mock_memory_resource resource{};

    test::record source_record{2, 2.0};
    test::record target_record{1, 1.0};
    auto src_meta = source_record.record_meta();
    encode(source_record.ref(), src_meta->value_offset(0), src_meta->at(0), asc, s);
    encode(source_record.ref(), src_meta->value_offset(1), src_meta->at(1), asc, s);
    s.reset();
    auto tgt_meta = target_record.record_meta();
    decode(s, tgt_meta->at(0), asc, target_record.ref(), tgt_meta->value_offset(0), &resource);
    decode(s, tgt_meta->at(1), asc, target_record.ref(), tgt_meta->value_offset(1), &resource);

    ASSERT_EQ(2, target_record.ref().get_value<std::int64_t>(tgt_meta->value_offset(0)));
    ASSERT_EQ(2.0, target_record.ref().get_value<double>(tgt_meta->value_offset(1)));
}

TEST_F(coder_test, encode_any) {
    std::string src(100, 0);
    std::string tgt(100, 0);
    kvs::stream s{src};
    kvs::stream t{tgt};

    mock_memory_resource resource{};

    test::record source_record{2, 2.0};
    test::record target_record{1, 1.0};
    auto src_meta = source_record.record_meta();

    executor::process::impl::expression::any src0{std::in_place_type<std::int64_t>, 2};
    executor::process::impl::expression::any src1{std::in_place_type<double>, 2.0};
    encode(src0, src_meta->at(0), asc, s);
    encode(src1, src_meta->at(1), asc, s);
    s.reset();
    auto tgt_meta = target_record.record_meta();
    decode(s, tgt_meta->at(0), asc, target_record.ref(), tgt_meta->value_offset(0), &resource);
    decode(s, tgt_meta->at(1), asc, target_record.ref(), tgt_meta->value_offset(1), &resource);

    ASSERT_EQ(2, target_record.ref().get_value<std::int64_t>(tgt_meta->value_offset(0)));
    ASSERT_EQ(2.0, target_record.ref().get_value<double>(tgt_meta->value_offset(1)));
}

TEST_F(coder_test, nullable) {
    mock_memory_resource resource{};
    {
        std::string src(100, 0);
        std::string tgt(100, 0);
        kvs::stream s{src};
        kvs::stream t{tgt};
        test::record source_record{2, 2.0};
        test::record target_record{1, 1.0};
        auto src_meta = source_record.record_meta();
        encode_nullable(source_record.ref(), src_meta->value_offset(0), src_meta->nullity_offset(0), src_meta->at(0), asc, s);
        encode_nullable(source_record.ref(), src_meta->value_offset(1), src_meta->nullity_offset(1), src_meta->at(1), asc, s);
        s.reset();
        auto tgt_meta = target_record.record_meta();
        decode_nullable(s, tgt_meta->at(0), asc, target_record.ref(), tgt_meta->value_offset(0), tgt_meta->nullity_offset(0), &resource);
        decode_nullable(s, tgt_meta->at(1), asc, target_record.ref(), tgt_meta->value_offset(1), tgt_meta->nullity_offset(1), &resource);

        ASSERT_EQ(2, *target_record.ref().get_if<std::int64_t>(tgt_meta->nullity_offset(0), tgt_meta->value_offset(0)));
        ASSERT_EQ(2.0, *target_record.ref().get_if<double>(tgt_meta->nullity_offset(1), tgt_meta->value_offset(1)));
    }
    {
        std::string src(100, 0);
        std::string tgt(100, 0);
        kvs::stream s{src};
        kvs::stream t{tgt};
        mock::basic_record source_record{mock::create_nullable_record<kind::int4, kind::int4, kind::float8, kind::float8>(std::forward_as_tuple(2, 2, 2.0, 2.0), {false, true, false, true})};
        mock::basic_record target_record{mock::create_nullable_record<kind::int4, kind::int4, kind::float8, kind::float8>(std::forward_as_tuple(1, 1, 1.0, 1.0), {false, false, false, false})};
        auto src_meta = source_record.record_meta();
        encode_nullable(source_record.ref(), src_meta->value_offset(0), src_meta->nullity_offset(0), src_meta->at(0), asc, s);
        encode_nullable(source_record.ref(), src_meta->value_offset(1), src_meta->nullity_offset(1), src_meta->at(1), asc, s);
        encode_nullable(source_record.ref(), src_meta->value_offset(2), src_meta->nullity_offset(2), src_meta->at(2), asc, s);
        encode_nullable(source_record.ref(), src_meta->value_offset(3), src_meta->nullity_offset(3), src_meta->at(3), asc, s);
        s.reset();
        auto tgt_meta = target_record.record_meta();
        decode_nullable(s, tgt_meta->at(0), asc, target_record.ref(), tgt_meta->value_offset(0), tgt_meta->nullity_offset(0), &resource);
        decode_nullable(s, tgt_meta->at(1), asc, target_record.ref(), tgt_meta->value_offset(1), tgt_meta->nullity_offset(1), &resource);
        decode_nullable(s, tgt_meta->at(2), asc, target_record.ref(), tgt_meta->value_offset(2), tgt_meta->nullity_offset(2), &resource);
        decode_nullable(s, tgt_meta->at(3), asc, target_record.ref(), tgt_meta->value_offset(3), tgt_meta->nullity_offset(3), &resource);

        ASSERT_EQ(2, *target_record.ref().get_if<std::int64_t>(tgt_meta->nullity_offset(0), tgt_meta->value_offset(0)));
        ASSERT_FALSE(target_record.ref().get_if<std::int64_t>(tgt_meta->nullity_offset(1), tgt_meta->value_offset(1)));
        ASSERT_EQ(2.0, *target_record.ref().get_if<double>(tgt_meta->nullity_offset(2), tgt_meta->value_offset(2)));
        ASSERT_FALSE(target_record.ref().get_if<double>(tgt_meta->nullity_offset(3), tgt_meta->value_offset(3)));
    }
}

TEST_F(coder_test, encode_any_nullable) {
    mock_memory_resource resource{};
    std::string src(100, 0);
    std::string tgt(100, 0);
    kvs::stream s{src};
    kvs::stream t{tgt};
    mock::basic_record source_record{mock::create_nullable_record<kind::int4, kind::int4, kind::float8, kind::float8>(std::forward_as_tuple(0, 0, 0.0, 0.0), {false, true, false, true})};
    mock::basic_record target_record{mock::create_nullable_record<kind::int4, kind::int4, kind::float8, kind::float8>(std::forward_as_tuple(1, 1, 1.0, 1.0), {false, false, false, false})};

    auto src_meta = source_record.record_meta();
    executor::process::impl::expression::any src0{std::in_place_type<std::int32_t>, 2};
    executor::process::impl::expression::any src1{};
    executor::process::impl::expression::any src2{std::in_place_type<double>, 2.0};
    executor::process::impl::expression::any src3{};

    encode_nullable(src0, src_meta->at(0), asc, s);
    encode_nullable(src1, src_meta->at(1), asc, s);
    encode_nullable(src2, src_meta->at(2), asc, s);
    encode_nullable(src3, src_meta->at(3), asc, s);
    s.reset();
    auto tgt_meta = target_record.record_meta();
    decode_nullable(s, tgt_meta->at(0), asc, target_record.ref(), tgt_meta->value_offset(0), tgt_meta->nullity_offset(0), &resource);
    decode_nullable(s, tgt_meta->at(1), asc, target_record.ref(), tgt_meta->value_offset(1), tgt_meta->nullity_offset(1), &resource);
    decode_nullable(s, tgt_meta->at(2), asc, target_record.ref(), tgt_meta->value_offset(2), tgt_meta->nullity_offset(2), &resource);
    decode_nullable(s, tgt_meta->at(3), asc, target_record.ref(), tgt_meta->value_offset(3), tgt_meta->nullity_offset(3), &resource);

    ASSERT_EQ(2, *target_record.ref().get_if<std::int64_t>(tgt_meta->nullity_offset(0), tgt_meta->value_offset(0)));
    ASSERT_FALSE(target_record.ref().get_if<std::int64_t>(tgt_meta->nullity_offset(1), tgt_meta->value_offset(1)));
    ASSERT_EQ(2.0, *target_record.ref().get_if<double>(tgt_meta->nullity_offset(2), tgt_meta->value_offset(2)));
    ASSERT_FALSE(target_record.ref().get_if<double>(tgt_meta->nullity_offset(3), tgt_meta->value_offset(3)));
}

TEST_F(coder_test, streams) {
    std::string src(100, 0);
    kvs::stream s{src};
    ASSERT_EQ(100, s.capacity());
}

}
