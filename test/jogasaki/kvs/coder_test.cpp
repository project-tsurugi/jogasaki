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
#include <jogasaki/kvs/coder.h>

#include <string>

#include <gtest/gtest.h>

#include <jogasaki/test_root.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/data/any.h>
#include <jogasaki/kvs/environment.h>
#include <jogasaki/utils/coder.h>

#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/test_utils/types.h>

namespace jogasaki::kvs {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;
using namespace std::chrono_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;
using namespace kvs::details;

class coder_test : public test_root {
public:
    using kind = meta::field_type_kind;
};

constexpr kvs::order asc = kvs::order::ascending;
constexpr kvs::order desc = kvs::order::descending;

constexpr kvs::coding_spec spec_asc = kvs::spec_key_ascending;
constexpr kvs::coding_spec spec_desc = kvs::spec_key_descending;
constexpr kvs::coding_spec spec_val = kvs::spec_value;

TEST_F(coder_test, simple) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    int_t<32> i32{1};
    int_t<64> i64{2};
    float_t<32> f32{3};
    float_t<64> f64{4};
    mock_memory_resource resource{};
    accessor::text txt{&resource, "ABC"sv};
    accessor::text txt2{&resource, "ABC"sv};
    EXPECT_EQ(status::ok, s.write(i32, asc));
    EXPECT_EQ(status::ok, s.write(f32, asc));
    EXPECT_EQ(status::ok, s.write(i64, asc));
    EXPECT_EQ(status::ok, s.write(f64, asc));
    EXPECT_EQ(status::ok, s.write(txt, asc, false, 3));

    auto rs = s.readable();
    ASSERT_EQ(i32, rs.read<std::int32_t>(asc, false));
    ASSERT_EQ(f32, rs.read<float>(asc, false));
    ASSERT_EQ(i64, rs.read<std::int64_t>(asc, false));
    ASSERT_EQ(f64, rs.read<double>(asc, false));
    ASSERT_EQ(txt2, rs.read<accessor::text>(asc, false, &resource));
}

TEST_F(coder_test, descendant) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    int_t<32> i32{1};
    int_t<64> i64{2};
    float_t<32> f32{3};
    float_t<64> f64{4};
    mock_memory_resource resource{};
    accessor::text txt{&resource, "ABC"sv};
    accessor::text txt2{&resource, "ABC"sv};
    EXPECT_EQ(status::ok, s.write(i32, desc));
    EXPECT_EQ(status::ok, s.write(f32, desc));
    EXPECT_EQ(status::ok, s.write(i64, desc));
    EXPECT_EQ(status::ok, s.write(f64, desc));
    EXPECT_EQ(status::ok, s.write(txt, desc, false, 3));

    auto rs = s.readable();
    ASSERT_EQ(i32, rs.read<std::int32_t>(desc, false));
    ASSERT_EQ(f32, rs.read<float>(desc, false));
    ASSERT_EQ(i64, rs.read<std::int64_t>(desc, false));
    ASSERT_EQ(f64, rs.read<double>(desc, false));
    ASSERT_EQ(txt, rs.read<accessor::text>(desc, false, &resource));
}

TEST_F(coder_test, i32_asc) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    int_t<32> i1{2};
    int_t<32> i2{-2};
    EXPECT_EQ(status::ok, s.write(i1, asc));
    EXPECT_EQ(status::ok, s.write(i2, asc));

    auto rs = s.readable();
    ASSERT_EQ(i1, rs.read<std::int32_t>(asc, false));
    ASSERT_EQ(i2, rs.read<std::int32_t>(asc, false));
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
    kvs::writable_stream s{buf};
    int_t<32> i1{2};
    int_t<32> i2{-2};
    EXPECT_EQ(status::ok, s.write(i1, desc));
    EXPECT_EQ(status::ok, s.write(i2, desc));

    auto rs = s.readable();

    ASSERT_EQ(i1, rs.read<std::int32_t>(desc, false));
    ASSERT_EQ(i2, rs.read<std::int32_t>(desc, false));
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
    kvs::writable_stream s{buf};
    int_t<64> i1{2};
    int_t<64> i2{-2};
    EXPECT_EQ(status::ok, s.write(i1, asc));
    EXPECT_EQ(status::ok, s.write(i2, asc));

    auto rs = s.readable();

    ASSERT_EQ(i1, rs.read<std::int64_t>(asc, false));
    ASSERT_EQ(i2, rs.read<std::int64_t>(asc, false));
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
    kvs::writable_stream s{buf};
    int_t<16> i1{2};
    int_t<16> i2{-2};
    EXPECT_EQ(status::ok, s.write(i1, asc));
    EXPECT_EQ(status::ok, s.write(i2, asc));

    auto rs = s.readable();

    ASSERT_EQ(i1, rs.read<std::int16_t>(asc, false));
    ASSERT_EQ(i2, rs.read<std::int16_t>(asc, false));
    EXPECT_EQ('\x80', buf[0]);
    EXPECT_EQ('\x02', buf[1]);
    EXPECT_EQ('\x7F', buf[2]);
    EXPECT_EQ('\xFE', buf[3]);
}

TEST_F(coder_test, i8_asc) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    int_t<8> i1{2};
    int_t<8> i2{-2};
    EXPECT_EQ(status::ok, s.write(i1, asc));
    EXPECT_EQ(status::ok, s.write(i2, asc));

    auto rs = s.readable();

    ASSERT_EQ(i1, rs.read<std::int8_t>(asc, false));
    ASSERT_EQ(i2, rs.read<std::int8_t>(asc, false));
    EXPECT_EQ('\x82', buf[0]);
    EXPECT_EQ('\x7E', buf[1]);
}

TEST_F(coder_test, f32_asc) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    float_t<32> f1{2};
    float_t<32> f2{-2};
    EXPECT_EQ(status::ok, s.write(f1, asc));
    EXPECT_EQ(status::ok, s.write(f2, asc));

    auto rs = s.readable();

    ASSERT_EQ(f1, rs.read<float>(asc, false));
    ASSERT_EQ(f2, rs.read<float>(asc, false));
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
    kvs::writable_stream s{buf};
    float_t<32> f1{2};
    float_t<32> f2{-2};
    EXPECT_EQ(status::ok, s.write(f1, desc));
    EXPECT_EQ(status::ok, s.write(f2, desc));

    auto rs = s.readable();

    ASSERT_EQ(f1, rs.read<float>(desc, false));
    ASSERT_EQ(f2, rs.read<float>(desc, false));
    EXPECT_EQ('\x3F', buf[0]);
    EXPECT_EQ('\xFF', buf[1]);
    EXPECT_EQ('\xFF', buf[2]);
    EXPECT_EQ('\xFF', buf[3]);
    EXPECT_EQ('\xC0', buf[4]);
    EXPECT_EQ('\x00', buf[5]);
    EXPECT_EQ('\x00', buf[6]);
    EXPECT_EQ('\x00', buf[7]);
}

std::string to_hex(float f) {
    std::uint32_t tmp{};
    std::memcpy(&tmp, &f, sizeof(tmp));
    std::stringstream ss{};
    ss << std::setprecision(8) << std::hex;
    ss << tmp;
    return ss.str();
}

std::string to_hex(double f) {
    std::uint64_t tmp{};
    std::memcpy(&tmp, &f, sizeof(tmp));
    std::stringstream ss{};
    ss << std::setprecision(16) << std::hex;
    ss << tmp;
    return ss.str();
}

TEST_F(coder_test, float_nan) {
    // verify nan are normaliezed and diagnostic code are removed
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    float_t<32> f1{std::nanf("1")};
    float_t<32> f2{std::nanf("2")};
    float_t<64> f3{std::nan("3")};
    float_t<64> f4{std::nan("4")};
    EXPECT_EQ(status::ok, s.write(f1, asc));
    EXPECT_EQ(status::ok, s.write(f2, desc));
    EXPECT_EQ(status::ok, s.write(f3, asc));
    EXPECT_EQ(status::ok, s.write(f4, desc));

    auto rs = s.readable();
    {
        auto f = rs.read<float>(asc, false);
        EXPECT_TRUE(std::isnan(f));
        EXPECT_EQ("7fc00000", to_hex(f));
    }
    {
        auto f = rs.read<float>(desc, false);
        EXPECT_TRUE(std::isnan(f));
        EXPECT_EQ("7fc00000", to_hex(f));
    }
    {
        auto f = rs.read<double>(asc, false);
        EXPECT_TRUE(std::isnan(f));
        EXPECT_EQ("7ff8000000000000", to_hex(f));
    }
    {
        auto f = rs.read<double>(desc, false);
        EXPECT_TRUE(std::isnan(f));
        EXPECT_EQ("7ff8000000000000", to_hex(f));
    }
}

TEST_F(coder_test, f64_asc) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    float_t<64> f1{2};
    float_t<64> f2{-2};
    EXPECT_EQ(status::ok, s.write(f1, asc));
    EXPECT_EQ(status::ok, s.write(f2, asc));

    auto rs = s.readable();

    ASSERT_EQ(f1, rs.read<double>(asc, false));
    ASSERT_EQ(f2, rs.read<double>(asc, false));
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

TEST_F(coder_test, text_asc) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    mock_memory_resource resource{};
    accessor::text txt{&resource, "ABC"sv};
    EXPECT_EQ(status::ok, s.write(txt, asc, false, 3));

    auto rs = s.readable();

    ASSERT_EQ(txt, rs.read<accessor::text>(asc, false, &resource));
    EXPECT_EQ('A', buf[0]);
    EXPECT_EQ('B', buf[1]);
    EXPECT_EQ('C', buf[2]);
    EXPECT_EQ('\x00', buf[3]);
    EXPECT_EQ('\x00', buf[4]);
    EXPECT_EQ('\x00', buf[5]);
    EXPECT_EQ('\x00', buf[6]);
}

char invert(unsigned char ch) {
    return static_cast<unsigned char>(-1)^ch;
}

TEST_F(coder_test, text_desc) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    mock_memory_resource resource{};
    accessor::text txt{&resource, "ABC"sv};
    EXPECT_EQ(status::ok, s.write(txt, desc, false, 3));
    auto rs = s.readable();

    ASSERT_EQ(txt, rs.read<accessor::text>(desc, false, &resource));
    EXPECT_EQ(invert('A'), buf[0]);
    EXPECT_EQ(invert('B'), buf[1]);
    EXPECT_EQ(invert('C'), buf[2]);
    EXPECT_EQ('\xFF', buf[3]);
    EXPECT_EQ('\xFF', buf[4]);
    EXPECT_EQ('\xFF', buf[5]);
    EXPECT_EQ('\xFF', buf[6]);
}

TEST_F(coder_test, empty_text_asc) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    mock_memory_resource resource{};
    accessor::text txt{&resource, ""sv};
    EXPECT_EQ(status::ok, s.write(txt, asc, false, 3));

    auto rs = s.readable();
    auto result = rs.read<accessor::text>(asc, false, &resource);
    ASSERT_EQ(txt, result);
    ASSERT_EQ(0, result.size());
    EXPECT_EQ('\x00', buf[0]);
    EXPECT_EQ('\x00', buf[1]);
    EXPECT_EQ('\x00', buf[2]);
    EXPECT_EQ('\x00', buf[3]);
}

TEST_F(coder_test, empty_text_desc) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    mock_memory_resource resource{};
    accessor::text txt{&resource, ""sv};
    EXPECT_EQ(status::ok, s.write(txt, desc, false, 3));

    auto rs = s.readable();
    auto result = rs.read<accessor::text>(desc, false, &resource);
    ASSERT_EQ(txt, result);
    ASSERT_EQ(0, result.size());
    EXPECT_EQ('\xFF', buf[0]);
    EXPECT_EQ('\xFF', buf[1]);
    EXPECT_EQ('\xFF', buf[2]);
    EXPECT_EQ('\xFF', buf[3]);
}

TEST_F(coder_test, text_non_variant_asc) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    mock_memory_resource resource{};
    accessor::text txt{&resource, "ABC"sv};
    EXPECT_EQ(status::ok, s.write(txt, asc, true, 6));
    auto rs = s.readable();

    accessor::text exp{&resource, "ABC   "sv};
    EXPECT_EQ(exp, rs.read<accessor::text>(asc, false, &resource));
    EXPECT_EQ('A', buf[0]);
    EXPECT_EQ('B', buf[1]);
    EXPECT_EQ('C', buf[2]);
    EXPECT_EQ('\x20', buf[3]);
    EXPECT_EQ('\x20', buf[4]);
    EXPECT_EQ('\x20', buf[5]);
    EXPECT_EQ('\x00', buf[6]);
    EXPECT_EQ('\x00', buf[7]);
    EXPECT_EQ('\x00', buf[8]);
    EXPECT_EQ('\x00', buf[9]);
}

TEST_F(coder_test, text_non_variant_desc) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    mock_memory_resource resource{};
    accessor::text txt{&resource, "ABC"sv};
    EXPECT_EQ(status::ok, s.write(txt, desc, true, 6));
    auto rs = s.readable();

    accessor::text exp{&resource, "ABC   "sv};
    EXPECT_EQ(exp, rs.read<accessor::text>(desc, false, &resource));
    EXPECT_EQ(invert('A'), buf[0]);
    EXPECT_EQ(invert('B'), buf[1]);
    EXPECT_EQ(invert('C'), buf[2]);
    EXPECT_EQ(invert('\x20'), buf[3]);
    EXPECT_EQ(invert('\x20'), buf[4]);
    EXPECT_EQ(invert('\x20'), buf[5]);
    EXPECT_EQ('\xFF', buf[6]);
    EXPECT_EQ('\xFF', buf[7]);
    EXPECT_EQ('\xFF', buf[8]);
    EXPECT_EQ('\xFF', buf[9]);
}

TEST_F(coder_test, binary) {
    // asc/desc ordering is not fully supported for binary type
    // currently re-cycling most of the logic for accessor::text
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    mock_memory_resource resource{};
    accessor::binary bin{&resource, "ABC"sv};
    EXPECT_EQ(status::ok, s.write(bin, asc, 10));

    auto rs = s.readable();
    auto result = rs.read<accessor::binary>(asc, false, &resource);
    EXPECT_EQ(3, result.size());
    ASSERT_EQ(bin, result);
    EXPECT_EQ('\x80', buf[0]);  // actually top bit is not needed because ordering is not yet supported for binary
    EXPECT_EQ('\x03', buf[1]);
    EXPECT_EQ('A', buf[2]);
    EXPECT_EQ('B', buf[3]);
    EXPECT_EQ('C', buf[4]);
    EXPECT_EQ('\x00', buf[5]);
    EXPECT_EQ('\x00', buf[6]);
}

TEST_F(coder_test, encode_decode) {
    std::string src(100, 0);
    std::string tgt(100, 0);
    kvs::writable_stream s{src};
    kvs::writable_stream t{tgt};

    mock_memory_resource resource{};

    test::record source_record{2, 2.0};
    test::record target_record{1, 1.0};
    auto src_meta = source_record.record_meta();
    EXPECT_EQ(status::ok, encode(source_record.ref(), src_meta->value_offset(0), src_meta->at(0), spec_asc, s));
    EXPECT_EQ(status::ok, encode(source_record.ref(), src_meta->value_offset(1), src_meta->at(1), spec_asc, s));
    auto rs = s.readable();
    auto tgt_meta = target_record.record_meta();
    EXPECT_EQ(status::ok, decode(rs, tgt_meta->at(0), spec_asc, target_record.ref(), tgt_meta->value_offset(0), &resource));
    EXPECT_EQ(status::ok, decode(rs, tgt_meta->at(1), spec_asc, target_record.ref(), tgt_meta->value_offset(1), &resource));

    ASSERT_EQ(2, target_record.ref().get_value<std::int64_t>(tgt_meta->value_offset(0)));
    ASSERT_EQ(2.0, target_record.ref().get_value<double>(tgt_meta->value_offset(1)));
}

TEST_F(coder_test, encode_decode_any) {
    std::string src(100, 0);
    std::string tgt(100, 0);
    kvs::writable_stream s{src};
    kvs::writable_stream t{tgt};

    mock_memory_resource resource{};

    test::record source_record{2, 2.0};
    test::record target_record{1, 1.0};
    auto src_meta = source_record.record_meta();

    data::any src0{std::in_place_type<std::int64_t>, 2};
    data::any src1{std::in_place_type<double>, 2.0};
    EXPECT_EQ(status::ok, encode(src0, src_meta->at(0), spec_asc, s));
    EXPECT_EQ(status::ok, encode(src1, src_meta->at(1), spec_asc, s));
    auto rs = s.readable();
    auto tgt_meta = target_record.record_meta();
    EXPECT_EQ(status::ok, decode(rs, tgt_meta->at(0), spec_asc, target_record.ref(), tgt_meta->value_offset(0), &resource));
    EXPECT_EQ(status::ok, decode(rs, tgt_meta->at(1), spec_asc, target_record.ref(), tgt_meta->value_offset(1), &resource));

    ASSERT_EQ(2, target_record.ref().get_value<std::int64_t>(tgt_meta->value_offset(0)));
    ASSERT_EQ(2.0, target_record.ref().get_value<double>(tgt_meta->value_offset(1)));

    rs = s.readable();
    data::any res{};
    EXPECT_EQ(status::ok, decode(rs, tgt_meta->at(0), spec_asc, res, &resource));
    EXPECT_EQ(2, res.to<std::int64_t>());
    EXPECT_EQ(status::ok, decode(rs, tgt_meta->at(1), spec_asc, res, &resource));
    EXPECT_EQ(2.0, res.to<double>());
}

TEST_F(coder_test, nullable) {
    mock_memory_resource resource{};
    {
        std::string src(100, 0);
        std::string tgt(100, 0);
        kvs::writable_stream s{src};
        kvs::writable_stream t{tgt};
        test::record source_record{2, 2.0};
        test::record target_record{1, 1.0};
        auto src_meta = source_record.record_meta();
        EXPECT_EQ(status::ok, encode_nullable(source_record.ref(), src_meta->value_offset(0), src_meta->nullity_offset(0), src_meta->at(0), spec_asc, s));
        EXPECT_EQ(status::ok, encode_nullable(source_record.ref(), src_meta->value_offset(1), src_meta->nullity_offset(1), src_meta->at(1), spec_asc, s));
        auto rs = s.readable();
        auto tgt_meta = target_record.record_meta();
        EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(0), spec_asc, target_record.ref(), tgt_meta->value_offset(0), tgt_meta->nullity_offset(0), &resource));
        EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(1), spec_asc, target_record.ref(), tgt_meta->value_offset(1), tgt_meta->nullity_offset(1), &resource));

        ASSERT_EQ(2, *target_record.ref().get_if<std::int64_t>(tgt_meta->nullity_offset(0), tgt_meta->value_offset(0)));
        ASSERT_EQ(2.0, *target_record.ref().get_if<double>(tgt_meta->nullity_offset(1), tgt_meta->value_offset(1)));
    }
    {
        std::string src(100, 0);
        std::string tgt(100, 0);
        kvs::writable_stream s{src};
        kvs::writable_stream t{tgt};
        mock::basic_record source_record{mock::create_nullable_record<kind::int4, kind::int4, kind::float8, kind::float8>(std::forward_as_tuple(2, 2, 2.0, 2.0), {false, true, false, true})};
        mock::basic_record target_record{mock::create_nullable_record<kind::int4, kind::int4, kind::float8, kind::float8>(std::forward_as_tuple(1, 1, 1.0, 1.0), {false, false, false, false})};
        auto src_meta = source_record.record_meta();
        EXPECT_EQ(status::ok, encode_nullable(source_record.ref(), src_meta->value_offset(0), src_meta->nullity_offset(0), src_meta->at(0), spec_asc, s));
        EXPECT_EQ(status::ok, encode_nullable(source_record.ref(), src_meta->value_offset(1), src_meta->nullity_offset(1), src_meta->at(1), spec_asc, s));
        EXPECT_EQ(status::ok, encode_nullable(source_record.ref(), src_meta->value_offset(2), src_meta->nullity_offset(2), src_meta->at(2), spec_asc, s));
        EXPECT_EQ(status::ok, encode_nullable(source_record.ref(), src_meta->value_offset(3), src_meta->nullity_offset(3), src_meta->at(3), spec_asc, s));
        auto rs = s.readable();
        auto tgt_meta = target_record.record_meta();
        EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(0), spec_asc, target_record.ref(), tgt_meta->value_offset(0), tgt_meta->nullity_offset(0), &resource));
        EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(1), spec_asc, target_record.ref(), tgt_meta->value_offset(1), tgt_meta->nullity_offset(1), &resource));
        EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(2), spec_asc, target_record.ref(), tgt_meta->value_offset(2), tgt_meta->nullity_offset(2), &resource));
        EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(3), spec_asc, target_record.ref(), tgt_meta->value_offset(3), tgt_meta->nullity_offset(3), &resource));

        ASSERT_EQ(2, *target_record.ref().get_if<std::int64_t>(tgt_meta->nullity_offset(0), tgt_meta->value_offset(0)));
        ASSERT_FALSE(target_record.ref().get_if<std::int64_t>(tgt_meta->nullity_offset(1), tgt_meta->value_offset(1)));
        ASSERT_EQ(2.0, *target_record.ref().get_if<double>(tgt_meta->nullity_offset(2), tgt_meta->value_offset(2)));
        ASSERT_FALSE(target_record.ref().get_if<double>(tgt_meta->nullity_offset(3), tgt_meta->value_offset(3)));
    }
}

TEST_F(coder_test, encode_decode_any_nullable) {
    mock_memory_resource resource{};
    std::string src(100, 0);
    std::string tgt(100, 0);
    kvs::writable_stream s{src};
    kvs::writable_stream t{tgt};
    mock::basic_record source_record{mock::create_nullable_record<kind::int4, kind::int4, kind::float8, kind::float8>(std::forward_as_tuple(0, 0, 0.0, 0.0), {false, true, false, true})};
    mock::basic_record target_record{mock::create_nullable_record<kind::int4, kind::int4, kind::float8, kind::float8>(std::forward_as_tuple(1, 1, 1.0, 1.0), {false, false, false, false})};

    auto src_meta = source_record.record_meta();
    data::any src0{std::in_place_type<std::int32_t>, 2};
    data::any src1{};
    data::any src2{std::in_place_type<double>, 2.0};
    data::any src3{};

    EXPECT_EQ(status::ok, encode_nullable(src0, src_meta->at(0), spec_asc, s));
    EXPECT_EQ(status::ok, encode_nullable(src1, src_meta->at(1), spec_asc, s));
    EXPECT_EQ(status::ok, encode_nullable(src2, src_meta->at(2), spec_asc, s));
    EXPECT_EQ(status::ok, encode_nullable(src3, src_meta->at(3), spec_asc, s));
    auto rs = s.readable();
    auto tgt_meta = target_record.record_meta();
    EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(0), spec_asc, target_record.ref(), tgt_meta->value_offset(0), tgt_meta->nullity_offset(0), &resource));
    EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(1), spec_asc, target_record.ref(), tgt_meta->value_offset(1), tgt_meta->nullity_offset(1), &resource));
    EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(2), spec_asc, target_record.ref(), tgt_meta->value_offset(2), tgt_meta->nullity_offset(2), &resource));
    EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(3), spec_asc, target_record.ref(), tgt_meta->value_offset(3), tgt_meta->nullity_offset(3), &resource));

    ASSERT_EQ(2, *target_record.ref().get_if<std::int32_t>(tgt_meta->nullity_offset(0), tgt_meta->value_offset(0)));
    ASSERT_FALSE(target_record.ref().get_if<std::int32_t>(tgt_meta->nullity_offset(1), tgt_meta->value_offset(1)));
    ASSERT_EQ(2.0, *target_record.ref().get_if<double>(tgt_meta->nullity_offset(2), tgt_meta->value_offset(2)));
    ASSERT_FALSE(target_record.ref().get_if<double>(tgt_meta->nullity_offset(3), tgt_meta->value_offset(3)));

    rs = s.readable();
    data::any res{};
    EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(0), spec_asc, res, &resource));
    EXPECT_EQ(2, res.to<std::int32_t>());
    EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(1), spec_asc, res, &resource));
    EXPECT_FALSE(res);
    EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(2), spec_asc, res, &resource));
    EXPECT_EQ(2.0, res.to<double>());
    EXPECT_EQ(status::ok, decode_nullable(rs, tgt_meta->at(3), spec_asc, res, &resource));
    EXPECT_FALSE(res);
}

TEST_F(coder_test, streams) {
    std::string src(100, 0);
    kvs::writable_stream s{src};
    ASSERT_EQ(100, s.capacity());
}

class bin {
public:
    bin(void* data, std::size_t len) : data_(data), len_(len) {}

    bool operator<(bin const& other) const noexcept {
        std::vector<unsigned char> a{static_cast<unsigned char*>(data_), static_cast<unsigned char*>(data_)+len_};
        std::vector<unsigned char> b{static_cast<unsigned char*>(other.data_), static_cast<unsigned char*>(other.data_)+other.len_};
        return a < b;
    }

    bool operator>(bin const& other) const noexcept {
        return other < *this;
    }

    void* data_{};
    std::size_t len_{};

    friend std::ostream& operator<<(std::ostream& out, bin const& b) {
        out << "length " << b.len_ << " data: ";
        for(std::size_t i=0; i < b.len_; ++i) {
            out << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(*(static_cast<unsigned char*>(b.data_)+i)) << " ";
        }
        return out;
    }
};

using kind = meta::field_type_kind;

void verify_order(meta::field_type type, data::any a, data::any b, bool only_nullable = false) {
    std::string src0(100, 0);
    std::string src1(100, 0);
    kvs::writable_stream s0{src0};
    kvs::writable_stream s1{src1};
    if (! only_nullable) {
        {
            // ascending non nullable
            EXPECT_EQ(status::ok, encode(a, type, spec_asc, s0));
            EXPECT_EQ(status::ok, encode(b, type, spec_asc, s1));
            EXPECT_LT(bin(src0.data(), s0.size()), bin(src1.data(), s1.size()));
        }
        s0.reset();
        s1.reset();
        {
            // descending non nullable
            EXPECT_EQ(status::ok, encode(a, type, spec_desc, s0));
            EXPECT_EQ(status::ok, encode(b, type, spec_desc, s1));
            EXPECT_GT(bin(src0.data(), s0.size()), bin(src1.data(), s1.size()));
        }
        s0.reset();
        s1.reset();
    }

    {
        // ascending nullable
        EXPECT_EQ(status::ok, encode_nullable(a, type, spec_asc, s0));
        EXPECT_EQ(status::ok, encode_nullable(b, type, spec_asc, s1));
        EXPECT_LT(bin(src0.data(), s0.size()), bin(src1.data(), s1.size()));
    }
    s0.reset();
    s1.reset();
    {
        // descending nullable
        EXPECT_EQ(status::ok, encode_nullable(a, type, spec_desc, s0));
        EXPECT_EQ(status::ok, encode_nullable(b, type, spec_desc, s1));
        EXPECT_GT(bin(src0.data(), s0.size()), bin(src1.data(), s1.size()));
    }
}

template <kind Kind>
void test_ordering() {
    std::string src0(100, 0);
    std::string src1(100, 0);
    std::string src2(100, 0);
    std::string src3(100, 0);
    kvs::writable_stream s0{src0};
    kvs::writable_stream s1{src1};
    kvs::writable_stream s2{src2};
    kvs::writable_stream s3{src3};
    data::any null{};
    data::any n1{std::in_place_type<runtime_t<Kind>>, -1};
    data::any z0{std::in_place_type<runtime_t<Kind>>, 0};
    data::any p1{std::in_place_type<runtime_t<Kind>>, 1};
    meta::field_type type{meta::field_enum_tag<Kind>};
    { SCOPED_TRACE("null < n1"); verify_order(type, null, n1, true); }
    { SCOPED_TRACE("n1 < z0"); verify_order(type, n1, z0); }
    { SCOPED_TRACE("z0 < p1"); verify_order(type, z0, p1); }
}

TEST_F(coder_test, i1_ordering) {
    test_ordering<kind::int1>();
}

TEST_F(coder_test, i2_ordering) {
    test_ordering<kind::int2>();
}

TEST_F(coder_test, i4_ordering) {
    test_ordering<kind::int4>();
}

TEST_F(coder_test, i8_ordering) {
    test_ordering<kind::int8>();
}

TEST_F(coder_test, f4_ordering) {
    test_ordering<kind::float4>();
}

TEST_F(coder_test, f8_ordering) {
    test_ordering<kind::float8>();
}

TEST_F(coder_test, text_ordering) {
    std::string src0(100, 0);
    std::string src1(100, 0);
    std::string src2(100, 0);
    std::string src3(100, 0);
    std::string src4(100, 0);
    std::string src5(100, 0);
    kvs::writable_stream s0{src0};
    kvs::writable_stream s1{src1};
    kvs::writable_stream s2{src2};
    kvs::writable_stream s3{src3};
    kvs::writable_stream s4{src4};
    kvs::writable_stream s5{src5};
    data::any c0{std::in_place_type<accessor::text>, text{""}};
    data::any c2{std::in_place_type<accessor::text>, text{"AA"}};
    data::any c3a{std::in_place_type<accessor::text>, text{"AAA"}};
    data::any c3b{std::in_place_type<accessor::text>, text{"AAB"}};
    data::any c5{std::in_place_type<accessor::text>, text{"BB"}};

    meta::field_type type{std::make_shared<meta::character_field_option>()};
    { SCOPED_TRACE("c0 < c2"); verify_order(type, c0, c2); }
    { SCOPED_TRACE("c2 < c3a"); verify_order(type, c2, c3a); }
    { SCOPED_TRACE("c3a < c3b"); verify_order(type, c3a, c3b); }
    { SCOPED_TRACE("c3b < c5"); verify_order(type, c3b, c5); }
}

TEST_F(coder_test, date_ordering) {
    std::string src0(100, 0);
    std::string src1(100, 0);
    std::string src2(100, 0);
    std::string src3(100, 0);
    std::string src4(100, 0);
    kvs::writable_stream s0{src0};
    kvs::writable_stream s1{src1};
    kvs::writable_stream s2{src2};
    kvs::writable_stream s3{src3};
    kvs::writable_stream s4{src4};
    data::any c0{std::in_place_type<rtype<ft::date>>, rtype<ft::date>{-2}};
    data::any c1{std::in_place_type<rtype<ft::date>>, rtype<ft::date>{-1}};
    data::any c2{std::in_place_type<rtype<ft::date>>, rtype<ft::date>{0}};
    data::any c3{std::in_place_type<rtype<ft::date>>, rtype<ft::date>{1}};
    data::any c4{std::in_place_type<rtype<ft::date>>, rtype<ft::date>{2}};
    meta::field_type type{meta::field_enum_tag<kind::date>};
    { SCOPED_TRACE("c0 < c1"); verify_order(type, c0, c1); }
    { SCOPED_TRACE("c1 < c2"); verify_order(type, c1, c2); }
    { SCOPED_TRACE("c2 < c3"); verify_order(type, c2, c3); }
    { SCOPED_TRACE("c3 < c4"); verify_order(type, c3, c4); }
}

TEST_F(coder_test, time_of_day_ordering) {
    std::string src0(100, 0);
    std::string src1(100, 0);
    std::string src2(100, 0);
    std::string src3(100, 0);
    kvs::writable_stream s0{src0};
    kvs::writable_stream s1{src1};
    kvs::writable_stream s2{src2};
    kvs::writable_stream s3{src3};
    data::any c0{std::in_place_type<rtype<ft::time_of_day>>, rtype<ft::time_of_day>{0ns}};
    data::any c1{std::in_place_type<rtype<ft::time_of_day>>, rtype<ft::time_of_day>{1ns}};
    data::any c2{std::in_place_type<rtype<ft::time_of_day>>, rtype<ft::time_of_day>{1ns*(24L*60*60*1000*1000*1000-2)}};
    data::any c3{std::in_place_type<rtype<ft::time_of_day>>, rtype<ft::time_of_day>{1ns*(24L*60*60*1000*1000*1000-1)}};

    meta::field_type type{std::make_shared<meta::time_of_day_field_option>()};
    { SCOPED_TRACE("c0 < c1"); verify_order(type, c0, c1); }
    { SCOPED_TRACE("c1 < c2"); verify_order(type, c1, c2); }
    { SCOPED_TRACE("c2 < c3"); verify_order(type, c2, c3); }
}

TEST_F(coder_test, time_point_ordering) {
    std::string src0(100, 0);
    std::string src1(100, 0);
    std::string src2(100, 0);
    std::string src3(100, 0);
    std::string src4(100, 0);
    kvs::writable_stream s0{src0};
    kvs::writable_stream s1{src1};
    kvs::writable_stream s2{src2};
    kvs::writable_stream s3{src3};
    kvs::writable_stream s4{src4};
    data::any c0{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{-2ns}};
    data::any c1{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{-1ns}};
    data::any c2{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{0ns}};
    data::any c3{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{1ns}};
    data::any c4{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{2ns}};

    meta::field_type type{std::make_shared<meta::time_point_field_option>()};
    { SCOPED_TRACE("c0 < c1"); verify_order(type, c0, c1); }
    { SCOPED_TRACE("c1 < c2"); verify_order(type, c1, c2); }
    { SCOPED_TRACE("c2 < c3"); verify_order(type, c2, c3); }
    { SCOPED_TRACE("c3 < c4"); verify_order(type, c3, c4); }
}

TEST_F(coder_test, time_point_ordering_with_only_secs) {
    std::string src0(100, 0);
    std::string src1(100, 0);
    std::string src2(100, 0);
    std::string src3(100, 0);
    std::string src4(100, 0);
    kvs::writable_stream s0{src0};
    kvs::writable_stream s1{src1};
    kvs::writable_stream s2{src2};
    kvs::writable_stream s3{src3};
    kvs::writable_stream s4{src4};
    data::any c0{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{-2s, 0ns}};
    data::any c1{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{-1s, 0ns}};
    data::any c2{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{0s, 0ns}};
    data::any c3{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{1s, 0ns}};
    data::any c4{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{2s, 0ns}};

    meta::field_type type{std::make_shared<meta::time_point_field_option>()};
    { SCOPED_TRACE("c0 < c1"); verify_order(type, c0, c1); }
    { SCOPED_TRACE("c1 < c2"); verify_order(type, c1, c2); }
    { SCOPED_TRACE("c2 < c3"); verify_order(type, c2, c3); }
    { SCOPED_TRACE("c3 < c4"); verify_order(type, c3, c4); }
}

TEST_F(coder_test, time_point_ordering_with_subsecs) {
    std::string src0(100, 0);
    std::string src1(100, 0);
    std::string src2(100, 0);
    std::string src3(100, 0);
    std::string src4(100, 0);
    kvs::writable_stream s0{src0};
    kvs::writable_stream s1{src1};
    kvs::writable_stream s2{src2};
    kvs::writable_stream s3{src3};
    kvs::writable_stream s4{src4};
    data::any c0{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{-1s, 100ms}};
    data::any c1{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{-1s, 200ms}};
    data::any c2{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{-1s, 900ms}};
    data::any c3{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{0s, 0ms}};
    data::any c4{std::in_place_type<rtype<ft::time_point>>, rtype<ft::time_point>{0s, 100ms}};
    meta::field_type type{std::make_shared<meta::time_point_field_option>()};
    { SCOPED_TRACE("c0 < c1"); verify_order(type, c0, c1); }
    { SCOPED_TRACE("c1 < c2"); verify_order(type, c1, c2); }
    { SCOPED_TRACE("c2 < c3"); verify_order(type, c2, c3); }
    { SCOPED_TRACE("c3 < c4"); verify_order(type, c3, c4); }
}

TEST_F(coder_test, decimal_ordering_simple) {
    auto opt = std::make_shared<meta::decimal_field_option>(6, 3);
    meta::field_type type{opt};
    data::any c0{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{-1, 0, 1, 2}}; // -100
    data::any c1{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{-1, 0, 10, 0}};  // -10
    data::any c2{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{0, 0, 0, 0}}; // 0
    data::any c3{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1, 0, 10, 0}};  // 10
    data::any c4{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1, 0, 1, 2}}; // 100

    { SCOPED_TRACE("c0 < c1"); verify_order(type, c0, c1); }
    { SCOPED_TRACE("c1 < c2"); verify_order(type, c1, c2); }
    { SCOPED_TRACE("c2 < c3"); verify_order(type, c2, c3); }
    { SCOPED_TRACE("c3 < c4"); verify_order(type, c3, c4); }
}

TEST_F(coder_test, decimal_simple) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    auto opt = std::make_shared<meta::decimal_field_option>(6, 3);
    auto v0 = rtype<ft::decimal>{-1, 0, 1, 2}; // -100
    auto v1 = rtype<ft::decimal>{-1, 0, 10, 0}; // -10
    auto v2 = rtype<ft::decimal>{0, 0, 0, 0}; // 0
    auto v3 = rtype<ft::decimal>{1, 0, 10, 0}; // 10
    auto v4 = rtype<ft::decimal>{1, 0, 1, 2}; // 100

    EXPECT_EQ(status::ok, s.write(v0, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v1, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v2, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v3, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v4, asc, *opt));

    ASSERT_EQ(3, utils::bytes_required_for_digits(6));
    std::size_t base = 0;
    EXPECT_EQ('\x7E', buf[base+0]);
    EXPECT_EQ('\x79', buf[base+1]);
    EXPECT_EQ('\x60', buf[base+2]);

    base = 3;
    EXPECT_EQ('\x7F', buf[base+0]);
    EXPECT_EQ('\xD8', buf[base+1]);
    EXPECT_EQ('\xF0', buf[base+2]);

    base = 6;
    EXPECT_EQ('\x80', buf[base+0]);
    EXPECT_EQ('\x00', buf[base+1]);
    EXPECT_EQ('\x00', buf[base+2]);

    base = 9;
    EXPECT_EQ('\x80', buf[base+0]);
    EXPECT_EQ('\x27', buf[base+1]);
    EXPECT_EQ('\x10', buf[base+2]);

    base = 12;
    EXPECT_EQ('\x81', buf[base+0]);
    EXPECT_EQ('\x86', buf[base+1]);
    EXPECT_EQ('\xA0', buf[base+2]);

    auto rs = s.readable();
    // writing and reading triple is not round-trip equal. Create Decimal to check equivalence.
    EXPECT_EQ(decimal::Decimal{v0}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v1}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v2}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v3}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v4}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(0, decimal::context.status());
}

TEST_F(coder_test, decimal_64bit_boundary_values) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    auto opt = std::make_shared<meta::decimal_field_option>(20, 0);

    auto v0 = rtype<ft::decimal>{-1, 1, 0, 0}; // -18446744073709551616
    auto v1 = rtype<ft::decimal>{-1, 0, 0xFFFFFFFFFFFFFFFFUL, 0}; // -18446744073709551615
    auto v2 = rtype<ft::decimal>{1, 0, 0x7FFFFFFFFFFFFFFFUL, 0}; // 9223372036854775807
    auto v3 = rtype<ft::decimal>{1, 0, 0xFFFFFFFFFFFFFFFFUL, 0}; // 18446744073709551615
    auto v4 = rtype<ft::decimal>{1, 1, 0, 0}; // 18446744073709551616
    EXPECT_EQ(status::ok, s.write(v0, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v1, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v2, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v3, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v4, asc, *opt));

    ASSERT_EQ(9, utils::bytes_required_for_digits(20));
    std::size_t base = 0;
    EXPECT_EQ('\x7F', buf[base+0]);
    EXPECT_EQ('\x00', buf[base+1]);
    EXPECT_EQ('\x00', buf[base+2]);
    EXPECT_EQ('\x00', buf[base+3]);
    EXPECT_EQ('\x00', buf[base+4]);
    EXPECT_EQ('\x00', buf[base+5]);
    EXPECT_EQ('\x00', buf[base+6]);
    EXPECT_EQ('\x00', buf[base+7]);
    EXPECT_EQ('\x00', buf[base+8]);

    base = 9;
    EXPECT_EQ('\x7F', buf[base+0]);
    EXPECT_EQ('\x00', buf[base+1]);
    EXPECT_EQ('\x00', buf[base+2]);
    EXPECT_EQ('\x00', buf[base+3]);
    EXPECT_EQ('\x00', buf[base+4]);
    EXPECT_EQ('\x00', buf[base+5]);
    EXPECT_EQ('\x00', buf[base+6]);
    EXPECT_EQ('\x00', buf[base+7]);
    EXPECT_EQ('\x01', buf[base+8]);

    base = 18;
    EXPECT_EQ('\x80', buf[base+0]);
    EXPECT_EQ('\x7F', buf[base+1]);
    EXPECT_EQ('\xFF', buf[base+2]);
    EXPECT_EQ('\xFF', buf[base+3]);
    EXPECT_EQ('\xFF', buf[base+4]);
    EXPECT_EQ('\xFF', buf[base+5]);
    EXPECT_EQ('\xFF', buf[base+6]);
    EXPECT_EQ('\xFF', buf[base+7]);
    EXPECT_EQ('\xFF', buf[base+8]);

    base = 27;
    EXPECT_EQ('\x80', buf[base+0]);
    EXPECT_EQ('\xFF', buf[base+1]);
    EXPECT_EQ('\xFF', buf[base+2]);
    EXPECT_EQ('\xFF', buf[base+3]);
    EXPECT_EQ('\xFF', buf[base+4]);
    EXPECT_EQ('\xFF', buf[base+5]);
    EXPECT_EQ('\xFF', buf[base+6]);
    EXPECT_EQ('\xFF', buf[base+7]);
    EXPECT_EQ('\xFF', buf[base+8]);

    base = 36;
    EXPECT_EQ('\x81', buf[base+0]);
    EXPECT_EQ('\x00', buf[base+1]);
    EXPECT_EQ('\x00', buf[base+2]);
    EXPECT_EQ('\x00', buf[base+3]);
    EXPECT_EQ('\x00', buf[base+4]);
    EXPECT_EQ('\x00', buf[base+5]);
    EXPECT_EQ('\x00', buf[base+6]);
    EXPECT_EQ('\x00', buf[base+7]);
    EXPECT_EQ('\x00', buf[base+8]);

    auto rs = s.readable();
    // writing and reading triple is not round-trip equal. Create Decimal to check equivalence.
    EXPECT_EQ(decimal::Decimal{v0}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v1}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v2}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v3}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v4}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(0, decimal::context.status());
}

TEST_F(coder_test, decimal_ordering_64bit_boundary_values) {
    auto opt = std::make_shared<meta::decimal_field_option>(20, 0);
    meta::field_type type{opt};
    data::any c0{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{-1, 1, 1, 0}}; // -18446744073709551617
    data::any c1{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{-1, 1, 0, 0}}; // -18446744073709551616
    data::any c2{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{-1, 0, 0xFFFFFFFFFFFFFFFFUL, 0}};  // -18446744073709551615
    data::any c3{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{-1, 0, 0xFFFFFFFFFFFFFFFEUL, 0}};  // -18446744073709551614
    data::any c4{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1, 0, 0, 0}};  // 0
    data::any c5{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1, 0, 0x7FFFFFFFFFFFFFFFUL, 0}}; // 9223372036854775807
    data::any c6{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1, 0, 0x8000000000000000UL, 0}}; // 9223372036854775808
    data::any c7{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1, 0, 0xFFFFFFFFFFFFFFFEUL, 0}};  // 18446744073709551614
    data::any c8{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1, 0, 0xFFFFFFFFFFFFFFFFUL, 0}};  // 18446744073709551615
    data::any c9{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1, 1, 0, 0}}; // 18446744073709551616
    data::any c10{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1, 1, 1, 0}}; // 18446744073709551617

    { SCOPED_TRACE("c0 < c1"); verify_order(type, c0, c1); }
    { SCOPED_TRACE("c1 < c2"); verify_order(type, c1, c2); }
    { SCOPED_TRACE("c2 < c3"); verify_order(type, c2, c3); }
    { SCOPED_TRACE("c3 < c4"); verify_order(type, c3, c4); }
    { SCOPED_TRACE("c4 < c5"); verify_order(type, c4, c5); }
    { SCOPED_TRACE("c5 < c6"); verify_order(type, c5, c6); }
    { SCOPED_TRACE("c6 < c7"); verify_order(type, c6, c7); }
    { SCOPED_TRACE("c7 < c8"); verify_order(type, c7, c8); }
    { SCOPED_TRACE("c8 < c9"); verify_order(type, c8, c9); }
    { SCOPED_TRACE("c9 < c10"); verify_order(type, c9, c10); }
}

TEST_F(coder_test, decimal_ordering_128bit_boundary_values) {
    auto opt = std::make_shared<meta::decimal_field_option>(39, 0);
    meta::field_type type{opt};
    data::any c0{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{-1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0}};
    data::any c1{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{-1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFEUL, 0}};
    data::any c2{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{-1, 0x8000000000000000UL, 0x0000000000000001UL, 0}};
    data::any c3{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{-1, 0x8000000000000000UL, 0x0000000000000000UL, 0}};
    data::any c4{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{-1, 0x7FFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0}};
    data::any c5{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{-1, 0x7FFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFEUL, 0}};
    data::any c6{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1,  0, 0, 0}};
    data::any c7{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1,  0x7FFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFEUL, 0}};
    data::any c8{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1,  0x7FFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0}};
    data::any c9{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1,  0x8000000000000000UL, 0x0000000000000000UL, 0}};
    data::any c10{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFEUL, 0}};
    data::any c11{std::in_place_type<rtype<ft::decimal>>, rtype<ft::decimal>{1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0}};
    { SCOPED_TRACE("c0 < c1"); verify_order(type, c0, c1); }
    { SCOPED_TRACE("c1 < c2"); verify_order(type, c1, c2); }
    { SCOPED_TRACE("c2 < c3"); verify_order(type, c2, c3); }
    { SCOPED_TRACE("c3 < c4"); verify_order(type, c3, c4); }
    { SCOPED_TRACE("c4 < c5"); verify_order(type, c4, c5); }
    { SCOPED_TRACE("c5 < c6"); verify_order(type, c5, c6); }
    { SCOPED_TRACE("c6 < c7"); verify_order(type, c6, c7); }
    { SCOPED_TRACE("c7 < c8"); verify_order(type, c7, c8); }
    { SCOPED_TRACE("c8 < c9"); verify_order(type, c8, c9); }
    { SCOPED_TRACE("c9 < c10"); verify_order(type, c9, c10); }
    { SCOPED_TRACE("c10 < c11"); verify_order(type, c10, c11); }
}

TEST_F(coder_test, decimal_128bit_boundary_values) {
    // not all 39-digit values are supported, but part of them can be realized by decimal triple
    // these values are internal use for now
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    auto opt = std::make_shared<meta::decimal_field_option>(39, 0);

    auto v0 = rtype<ft::decimal>{-1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0};
    auto v1 = rtype<ft::decimal>{-1, 0x8000000000000000UL, 0x0000000000000000UL, 0};
    auto v2 = rtype<ft::decimal>{-1, 0x7FFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0};
    auto v3 = rtype<ft::decimal>{1, 0x8000000000000000UL,  0x0000000000000000UL, 0};
    auto v4 = rtype<ft::decimal>{1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0};

    EXPECT_EQ(status::ok, s.write(v0, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v1, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v2, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v3, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v4, asc, *opt));

    std::size_t base = 0;
    ASSERT_EQ(17, utils::bytes_required_for_digits(39));
    EXPECT_EQ('\x7F', buf[base+0]);
    EXPECT_EQ('\x00', buf[base+1]);
    EXPECT_EQ('\x00', buf[base+2]);
    EXPECT_EQ('\x00', buf[base+3]);
    EXPECT_EQ('\x00', buf[base+4]);
    EXPECT_EQ('\x00', buf[base+5]);
    EXPECT_EQ('\x00', buf[base+6]);
    EXPECT_EQ('\x00', buf[base+7]);
    EXPECT_EQ('\x00', buf[base+8]);
    EXPECT_EQ('\x00', buf[base+9]);
    EXPECT_EQ('\x00', buf[base+10]);
    EXPECT_EQ('\x00', buf[base+11]);
    EXPECT_EQ('\x00', buf[base+12]);
    EXPECT_EQ('\x00', buf[base+13]);
    EXPECT_EQ('\x00', buf[base+14]);
    EXPECT_EQ('\x00', buf[base+15]);
    EXPECT_EQ('\x01', buf[base+16]);

    base = 17;
    EXPECT_EQ('\x7F', buf[base+0]);
    EXPECT_EQ('\x80', buf[base+1]);
    EXPECT_EQ('\x00', buf[base+2]);
    EXPECT_EQ('\x00', buf[base+3]);
    EXPECT_EQ('\x00', buf[base+4]);
    EXPECT_EQ('\x00', buf[base+5]);
    EXPECT_EQ('\x00', buf[base+6]);
    EXPECT_EQ('\x00', buf[base+7]);
    EXPECT_EQ('\x00', buf[base+8]);
    EXPECT_EQ('\x00', buf[base+9]);
    EXPECT_EQ('\x00', buf[base+10]);
    EXPECT_EQ('\x00', buf[base+11]);
    EXPECT_EQ('\x00', buf[base+12]);
    EXPECT_EQ('\x00', buf[base+13]);
    EXPECT_EQ('\x00', buf[base+14]);
    EXPECT_EQ('\x00', buf[base+15]);
    EXPECT_EQ('\x00', buf[base+16]);

    base = 34;
    EXPECT_EQ('\x7F', buf[base+0]);
    EXPECT_EQ('\x80', buf[base+1]);
    EXPECT_EQ('\x00', buf[base+2]);
    EXPECT_EQ('\x00', buf[base+3]);
    EXPECT_EQ('\x00', buf[base+4]);
    EXPECT_EQ('\x00', buf[base+5]);
    EXPECT_EQ('\x00', buf[base+6]);
    EXPECT_EQ('\x00', buf[base+7]);
    EXPECT_EQ('\x00', buf[base+8]);
    EXPECT_EQ('\x00', buf[base+9]);
    EXPECT_EQ('\x00', buf[base+10]);
    EXPECT_EQ('\x00', buf[base+11]);
    EXPECT_EQ('\x00', buf[base+12]);
    EXPECT_EQ('\x00', buf[base+13]);
    EXPECT_EQ('\x00', buf[base+14]);
    EXPECT_EQ('\x00', buf[base+15]);
    EXPECT_EQ('\x01', buf[base+16]);

    base = 51;
    EXPECT_EQ('\x80', buf[base+0]);
    EXPECT_EQ('\x80', buf[base+1]);
    EXPECT_EQ('\x00', buf[base+2]);
    EXPECT_EQ('\x00', buf[base+3]);
    EXPECT_EQ('\x00', buf[base+4]);
    EXPECT_EQ('\x00', buf[base+5]);
    EXPECT_EQ('\x00', buf[base+6]);
    EXPECT_EQ('\x00', buf[base+7]);
    EXPECT_EQ('\x00', buf[base+8]);
    EXPECT_EQ('\x00', buf[base+9]);
    EXPECT_EQ('\x00', buf[base+10]);
    EXPECT_EQ('\x00', buf[base+11]);
    EXPECT_EQ('\x00', buf[base+12]);
    EXPECT_EQ('\x00', buf[base+13]);
    EXPECT_EQ('\x00', buf[base+14]);
    EXPECT_EQ('\x00', buf[base+15]);
    EXPECT_EQ('\x00', buf[base+16]);

    base = 68;
    EXPECT_EQ('\x80', buf[base+0]);
    EXPECT_EQ('\xFF', buf[base+1]);
    EXPECT_EQ('\xFF', buf[base+2]);
    EXPECT_EQ('\xFF', buf[base+3]);
    EXPECT_EQ('\xFF', buf[base+4]);
    EXPECT_EQ('\xFF', buf[base+5]);
    EXPECT_EQ('\xFF', buf[base+6]);
    EXPECT_EQ('\xFF', buf[base+7]);
    EXPECT_EQ('\xFF', buf[base+8]);
    EXPECT_EQ('\xFF', buf[base+9]);
    EXPECT_EQ('\xFF', buf[base+10]);
    EXPECT_EQ('\xFF', buf[base+11]);
    EXPECT_EQ('\xFF', buf[base+12]);
    EXPECT_EQ('\xFF', buf[base+13]);
    EXPECT_EQ('\xFF', buf[base+14]);
    EXPECT_EQ('\xFF', buf[base+15]);
    EXPECT_EQ('\xFF', buf[base+16]);

    auto rs = s.readable();
    // writing and reading triple is not round-trip equal. Create Decimal to check equivalence.
    EXPECT_EQ(decimal::Decimal{v0}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v1}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v2}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v3}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v4}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(0, decimal::context.status());
}

TEST_F(coder_test, decimal_max_boundary_values) {
    // test boundary values around 38-digits decimal maximum
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    auto opt = std::make_shared<meta::decimal_field_option>(38, 0);

    auto v0 = rtype<ft::decimal>{-1, 0x4B3B4CA85A86C47AUL, 0x098A223FFFFFFFFFUL, 0}; // -999....9 (38 digits)
    auto v1 = rtype<ft::decimal>{-1, 0x4B3B4CA85A86C47AUL, 0x098A223FFFFFFFFEUL, 0}; // -999....8 (38 digits)
    auto v2 = rtype<ft::decimal>{0, 0, 0, 0}; // 0
    auto v3 = rtype<ft::decimal>{1, 0x4B3B4CA85A86C47AUL, 0x098A223FFFFFFFFEUL, 0}; // +999....8 (38 digits)
    auto v4 = rtype<ft::decimal>{1, 0x4B3B4CA85A86C47AUL, 0x098A223FFFFFFFFFUL, 0}; // +999....9 (38 digits)

    EXPECT_EQ(status::ok, s.write(v0, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v1, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v2, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v3, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v4, asc, *opt));

    auto rs = s.readable();
    // writing and reading triple is not round-trip equal. Create Decimal to check equivalence.
    EXPECT_EQ(decimal::Decimal{v0}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v1}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v2}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v3}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(decimal::Decimal{v4}, decimal::Decimal{rs.read<rtype<ft::decimal>>(asc, false, *opt)});
    EXPECT_EQ(0, decimal::context.status());
}

TEST_F(coder_test, different_triple_for_same_value) {
    // triple has multiple representations for the same value, but they will be same after serialization
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    auto opt = std::make_shared<meta::decimal_field_option>(6, 3);
    auto v0 = rtype<ft::decimal>{-1, 0, 1, 2}; // -100
    auto v1 = rtype<ft::decimal>{-1, 0, 10, 1};
    auto v2 = rtype<ft::decimal>{-1, 0, 100, 0};

    EXPECT_EQ(status::ok, s.write(v0, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v1, asc, *opt));
    EXPECT_EQ(status::ok, s.write(v2, asc, *opt));

    auto rs = s.readable();
    // writing and reading triple is not round-trip equal. Create Decimal to check equivalence.
    auto r0 = rs.read<rtype<ft::decimal>>(asc, false, *opt);
    auto r1 = rs.read<rtype<ft::decimal>>(asc, false, *opt);
    auto r2 = rs.read<rtype<ft::decimal>>(asc, false, *opt);
    EXPECT_EQ(r0, r1);
    EXPECT_EQ(r1, r2);
    EXPECT_EQ(0, decimal::context.status());
}
}
