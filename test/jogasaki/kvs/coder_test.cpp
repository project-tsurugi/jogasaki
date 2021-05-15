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
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/readable_stream.h>
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
    s.write(i32, asc);
    s.write(f32, asc);
    s.write(i64, asc);
    s.write(f64, asc);
    s.write(txt, asc);

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
    s.write(i32, desc);
    s.write(f32, desc);
    s.write(i64, desc);
    s.write(f64, desc);
    s.write(txt, desc);

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
    s.write(i1, asc);
    s.write(i2, asc);

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
    s.write(i1, desc);
    s.write(i2, desc);

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
    s.write(i1, asc);
    s.write(i2, asc);

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
    s.write(i1, asc);
    s.write(i2, asc);

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
    s.write(i1, asc);
    s.write(i2, asc);

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
    s.write(f1, asc);
    s.write(f2, asc);

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
    s.write(f1, desc);
    s.write(f2, desc);

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

TEST_F(coder_test, float_nan) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    float_t<32> f1{std::nanf("1")};
    float_t<32> f2{std::nanf("2")};
    float_t<64> f3{std::nan("3")};
    float_t<64> f4{std::nan("4")};
    s.write(f1, asc);
    s.write(f2, desc);
    s.write(f3, asc);
    s.write(f4, desc);

    auto rs = s.readable();

    ASSERT_TRUE(std::isnan(rs.read<float>(asc, false)));
    ASSERT_TRUE(std::isnan(rs.read<float>(desc, false)));
    ASSERT_TRUE(std::isnan(rs.read<double>(asc, false)));
    ASSERT_TRUE(std::isnan(rs.read<double>(desc, false)));
}

TEST_F(coder_test, f64_asc) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    float_t<64> f1{2};
    float_t<64> f2{-2};
    s.write(f1, asc);
    s.write(f2, asc);

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

TEST_F(coder_test, text) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    mock_memory_resource resource{};
    accessor::text txt{&resource, "ABC"sv};
    s.write(txt, asc);

    auto rs = s.readable();

    ASSERT_EQ(txt, rs.read<accessor::text>(asc, false, &resource));
    EXPECT_EQ('\x80', buf[0]);
    EXPECT_EQ('\x03', buf[1]);
    EXPECT_EQ('A', buf[2]);
    EXPECT_EQ('B', buf[3]);
    EXPECT_EQ('C', buf[4]);
}

TEST_F(coder_test, empty_text) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    mock_memory_resource resource{};
    accessor::text txt{&resource, ""sv};
    s.write(txt, asc);

    auto rs = s.readable();
    auto result = rs.read<accessor::text>(asc, false, &resource);
    ASSERT_EQ(txt, result);
    ASSERT_EQ(0, result.size());
    EXPECT_EQ('\x80', buf[0]);
    EXPECT_EQ('\x00', buf[1]);
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
    encode(source_record.ref(), src_meta->value_offset(0), src_meta->at(0), spec_asc, s);
    encode(source_record.ref(), src_meta->value_offset(1), src_meta->at(1), spec_asc, s);
    auto rs = s.readable();
    auto tgt_meta = target_record.record_meta();
    decode(rs, tgt_meta->at(0), spec_asc, target_record.ref(), tgt_meta->value_offset(0), &resource);
    decode(rs, tgt_meta->at(1), spec_asc, target_record.ref(), tgt_meta->value_offset(1), &resource);

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

    executor::process::impl::expression::any src0{std::in_place_type<std::int64_t>, 2};
    executor::process::impl::expression::any src1{std::in_place_type<double>, 2.0};
    encode(src0, src_meta->at(0), spec_asc, s);
    encode(src1, src_meta->at(1), spec_asc, s);
    auto rs = s.readable();
    auto tgt_meta = target_record.record_meta();
    decode(rs, tgt_meta->at(0), spec_asc, target_record.ref(), tgt_meta->value_offset(0), &resource);
    decode(rs, tgt_meta->at(1), spec_asc, target_record.ref(), tgt_meta->value_offset(1), &resource);

    ASSERT_EQ(2, target_record.ref().get_value<std::int64_t>(tgt_meta->value_offset(0)));
    ASSERT_EQ(2.0, target_record.ref().get_value<double>(tgt_meta->value_offset(1)));

    rs = s.readable();
    executor::process::impl::expression::any res{};
    decode(rs, tgt_meta->at(0), spec_asc, res, &resource);
    EXPECT_EQ(2, res.to<std::int64_t>());
    decode(rs, tgt_meta->at(1), spec_asc, res, &resource);
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
        encode_nullable(source_record.ref(), src_meta->value_offset(0), src_meta->nullity_offset(0), src_meta->at(0), spec_asc, s);
        encode_nullable(source_record.ref(), src_meta->value_offset(1), src_meta->nullity_offset(1), src_meta->at(1), spec_asc, s);
        auto rs = s.readable();
        auto tgt_meta = target_record.record_meta();
        decode_nullable(rs, tgt_meta->at(0), spec_asc, target_record.ref(), tgt_meta->value_offset(0), tgt_meta->nullity_offset(0), &resource);
        decode_nullable(rs, tgt_meta->at(1), spec_asc, target_record.ref(), tgt_meta->value_offset(1), tgt_meta->nullity_offset(1), &resource);

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
        encode_nullable(source_record.ref(), src_meta->value_offset(0), src_meta->nullity_offset(0), src_meta->at(0), spec_asc, s);
        encode_nullable(source_record.ref(), src_meta->value_offset(1), src_meta->nullity_offset(1), src_meta->at(1), spec_asc, s);
        encode_nullable(source_record.ref(), src_meta->value_offset(2), src_meta->nullity_offset(2), src_meta->at(2), spec_asc, s);
        encode_nullable(source_record.ref(), src_meta->value_offset(3), src_meta->nullity_offset(3), src_meta->at(3), spec_asc, s);
        auto rs = s.readable();
        auto tgt_meta = target_record.record_meta();
        decode_nullable(rs, tgt_meta->at(0), spec_asc, target_record.ref(), tgt_meta->value_offset(0), tgt_meta->nullity_offset(0), &resource);
        decode_nullable(rs, tgt_meta->at(1), spec_asc, target_record.ref(), tgt_meta->value_offset(1), tgt_meta->nullity_offset(1), &resource);
        decode_nullable(rs, tgt_meta->at(2), spec_asc, target_record.ref(), tgt_meta->value_offset(2), tgt_meta->nullity_offset(2), &resource);
        decode_nullable(rs, tgt_meta->at(3), spec_asc, target_record.ref(), tgt_meta->value_offset(3), tgt_meta->nullity_offset(3), &resource);

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
    executor::process::impl::expression::any src0{std::in_place_type<std::int32_t>, 2};
    executor::process::impl::expression::any src1{};
    executor::process::impl::expression::any src2{std::in_place_type<double>, 2.0};
    executor::process::impl::expression::any src3{};

    encode_nullable(src0, src_meta->at(0), spec_asc, s);
    encode_nullable(src1, src_meta->at(1), spec_asc, s);
    encode_nullable(src2, src_meta->at(2), spec_asc, s);
    encode_nullable(src3, src_meta->at(3), spec_asc, s);
    auto rs = s.readable();
    auto tgt_meta = target_record.record_meta();
    decode_nullable(rs, tgt_meta->at(0), spec_asc, target_record.ref(), tgt_meta->value_offset(0), tgt_meta->nullity_offset(0), &resource);
    decode_nullable(rs, tgt_meta->at(1), spec_asc, target_record.ref(), tgt_meta->value_offset(1), tgt_meta->nullity_offset(1), &resource);
    decode_nullable(rs, tgt_meta->at(2), spec_asc, target_record.ref(), tgt_meta->value_offset(2), tgt_meta->nullity_offset(2), &resource);
    decode_nullable(rs, tgt_meta->at(3), spec_asc, target_record.ref(), tgt_meta->value_offset(3), tgt_meta->nullity_offset(3), &resource);

    ASSERT_EQ(2, *target_record.ref().get_if<std::int32_t>(tgt_meta->nullity_offset(0), tgt_meta->value_offset(0)));
    ASSERT_FALSE(target_record.ref().get_if<std::int32_t>(tgt_meta->nullity_offset(1), tgt_meta->value_offset(1)));
    ASSERT_EQ(2.0, *target_record.ref().get_if<double>(tgt_meta->nullity_offset(2), tgt_meta->value_offset(2)));
    ASSERT_FALSE(target_record.ref().get_if<double>(tgt_meta->nullity_offset(3), tgt_meta->value_offset(3)));

    rs = s.readable();
    executor::process::impl::expression::any res{};
    decode_nullable(rs, tgt_meta->at(0), spec_asc, res, &resource);
    EXPECT_EQ(2, res.to<std::int32_t>());
    decode_nullable(rs, tgt_meta->at(1), spec_asc, res, &resource);
    EXPECT_FALSE(res);
    decode_nullable(rs, tgt_meta->at(2), spec_asc, res, &resource);
    EXPECT_EQ(2.0, res.to<double>());
    decode_nullable(rs, tgt_meta->at(3), spec_asc, res, &resource);
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
    executor::process::impl::expression::any n1{std::in_place_type<runtime_t<Kind>>, -1};
    executor::process::impl::expression::any z0{std::in_place_type<runtime_t<Kind>>, 0};
    executor::process::impl::expression::any p1{std::in_place_type<runtime_t<Kind>>, 1};
    {
        // ascending non nullable
        encode(n1, meta::field_type{enum_tag<Kind>}, spec_asc, s1);
        encode(z0, meta::field_type{enum_tag<Kind>}, spec_asc, s2);
        encode(p1, meta::field_type{enum_tag<Kind>}, spec_asc, s3);
        EXPECT_LT(bin(src1.data(), s1.size()), bin(src2.data(), s2.size()));
        EXPECT_LT(bin(src2.data(), s2.size()), bin(src3.data(), s3.size()));
    }
    s0.reset();
    s1.reset();
    s2.reset();
    s3.reset();
    {
        // descending non nullable
        encode(n1, meta::field_type{enum_tag<Kind>}, spec_desc, s1);
        encode(z0, meta::field_type{enum_tag<Kind>}, spec_desc, s2);
        encode(p1, meta::field_type{enum_tag<Kind>}, spec_desc, s3);
        EXPECT_GT(bin(src1.data(), s1.size()), bin(src2.data(), s2.size()));
        EXPECT_GT(bin(src2.data(), s2.size()), bin(src3.data(), s3.size()));
    }
    s0.reset();
    s1.reset();
    s2.reset();
    s3.reset();
    {
        // ascending nullable
        encode_nullable({}, meta::field_type{enum_tag<Kind>}, spec_asc, s0);
        encode_nullable(n1, meta::field_type{enum_tag<Kind>}, spec_asc, s1);
        encode_nullable(z0, meta::field_type{enum_tag<Kind>}, spec_asc, s2);
        encode_nullable(p1, meta::field_type{enum_tag<Kind>}, spec_asc, s3);
        EXPECT_LT(bin(src0.data(), s0.size()), bin(src1.data(), s1.size()));
        EXPECT_LT(bin(src1.data(), s1.size()), bin(src2.data(), s2.size()));
        EXPECT_LT(bin(src2.data(), s2.size()), bin(src3.data(), s3.size()));
    }
    s0.reset();
    s1.reset();
    s2.reset();
    s3.reset();
    {
        // descending nullable
        encode_nullable({}, meta::field_type{enum_tag<Kind>}, spec_desc, s0);
        encode_nullable(n1, meta::field_type{enum_tag<Kind>}, spec_desc, s1);
        encode_nullable(z0, meta::field_type{enum_tag<Kind>}, spec_desc, s2);
        encode_nullable(p1, meta::field_type{enum_tag<Kind>}, spec_desc, s3);
        EXPECT_GT(bin(src0.data(), s0.size()), bin(src1.data(), s1.size()));
        EXPECT_GT(bin(src1.data(), s1.size()), bin(src2.data(), s2.size()));
        EXPECT_GT(bin(src2.data(), s2.size()), bin(src3.data(), s3.size()));
    }
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
    kvs::writable_stream s0{src0};
    kvs::writable_stream s1{src1};
    kvs::writable_stream s2{src2};
    kvs::writable_stream s3{src3};
    kvs::writable_stream s4{src4};
    executor::process::impl::expression::any c0{std::in_place_type<accessor::text>, text{""}};
    executor::process::impl::expression::any c2{std::in_place_type<accessor::text>, text{"AA"}};
    executor::process::impl::expression::any c3a{std::in_place_type<accessor::text>, text{"AAA"}};
    executor::process::impl::expression::any c3b{std::in_place_type<accessor::text>, text{"AAB"}};
    {
        // ascending non nullable
        encode(c0, meta::field_type{enum_tag<kind::character>}, spec_asc, s1);
        encode(c2, meta::field_type{enum_tag<kind::character>}, spec_asc, s2);
        encode(c3a, meta::field_type{enum_tag<kind::character>}, spec_asc, s3);
        encode(c3b, meta::field_type{enum_tag<kind::character>}, spec_asc, s4);
        EXPECT_LT(bin(src1.data(), s1.size()), bin(src2.data(), s2.size()));
        EXPECT_LT(bin(src2.data(), s2.size()), bin(src3.data(), s3.size()));
        EXPECT_LT(bin(src3.data(), s3.size()), bin(src4.data(), s4.size()));
    }
    s0.reset();
    s1.reset();
    s2.reset();
    s3.reset();
    s4.reset();
    {
        // descending non nullable
        encode(c0, meta::field_type{enum_tag<kind::character>}, spec_desc, s1);
        encode(c2, meta::field_type{enum_tag<kind::character>}, spec_desc, s2);
        encode(c3a, meta::field_type{enum_tag<kind::character>}, spec_desc, s3);
        encode(c3b, meta::field_type{enum_tag<kind::character>}, spec_desc, s4);
        EXPECT_GT(bin(src1.data(), s1.size()), bin(src2.data(), s2.size()));
        EXPECT_GT(bin(src2.data(), s2.size()), bin(src3.data(), s3.size()));
        EXPECT_GT(bin(src3.data(), s3.size()), bin(src4.data(), s4.size()));
    }
    s0.reset();
    s1.reset();
    s2.reset();
    s3.reset();
    s4.reset();
    {
        // ascending nullable
        encode_nullable(c0, meta::field_type{enum_tag<kind::character>}, spec_asc, s1);
        encode_nullable(c2, meta::field_type{enum_tag<kind::character>}, spec_asc, s2);
        encode_nullable(c3a, meta::field_type{enum_tag<kind::character>}, spec_asc, s3);
        encode_nullable(c3b, meta::field_type{enum_tag<kind::character>}, spec_asc, s4);
        EXPECT_LT(bin(src1.data(), s1.size()), bin(src2.data(), s2.size()));
        EXPECT_LT(bin(src2.data(), s2.size()), bin(src3.data(), s3.size()));
        EXPECT_LT(bin(src3.data(), s3.size()), bin(src4.data(), s4.size()));
    }
    s0.reset();
    s1.reset();
    s2.reset();
    s3.reset();
    s4.reset();
    {
        // descending nullable
        encode_nullable({}, meta::field_type{enum_tag<kind::character>}, spec_desc, s0);
        encode_nullable(c0, meta::field_type{enum_tag<kind::character>}, spec_desc, s1);
        encode_nullable(c2, meta::field_type{enum_tag<kind::character>}, spec_desc, s2);
        encode_nullable(c3a, meta::field_type{enum_tag<kind::character>}, spec_desc, s3);
        encode_nullable(c3b, meta::field_type{enum_tag<kind::character>}, spec_desc, s4);
        EXPECT_GT(bin(src0.data(), s0.size()), bin(src1.data(), s1.size()));
        EXPECT_GT(bin(src1.data(), s1.size()), bin(src2.data(), s2.size()));
        EXPECT_GT(bin(src2.data(), s2.size()), bin(src3.data(), s3.size()));
        EXPECT_GT(bin(src3.data(), s3.size()), bin(src4.data(), s4.size()));
    }
}
}
