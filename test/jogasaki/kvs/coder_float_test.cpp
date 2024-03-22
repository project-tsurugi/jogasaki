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

class coder_float_test : public test_root {
public:
    using kind = meta::field_type_kind;
};

constexpr kvs::order asc = kvs::order::ascending;
constexpr kvs::order desc = kvs::order::descending;

constexpr kvs::coding_spec spec_asc = kvs::spec_key_ascending;
constexpr kvs::coding_spec spec_desc = kvs::spec_key_descending;
constexpr kvs::coding_spec spec_val = kvs::spec_value;

std::string to_hex(std::string_view f) {
    std::stringstream ss{};
    ss << std::hex << std::setfill('0');
    for(auto&& d : f) {
        ss << std::setw(2) << static_cast<int>(static_cast<unsigned char>(d));
    }
    return ss.str();
}

template <class T>
std::string write_float(T f, order o) {
    std::string buf(100, 0);
    kvs::writable_stream s{buf};
    if(status::ok != s.write(f, o)) {
        throw std::runtime_error("write failed");
    }
    return std::string{std::string_view{buf.data(), s.size()}};
}

TEST_F(coder_float_test, nan) {
    EXPECT_EQ("ffc00000", to_hex(write_float(std::numeric_limits<float>::quiet_NaN(), asc)));
    EXPECT_EQ("ffc00000", to_hex(write_float(-std::numeric_limits<float>::quiet_NaN(), asc)));
    EXPECT_EQ("ffc00000", to_hex(write_float(std::nanf("111"), asc)));
    EXPECT_EQ("fff8000000000000", to_hex(write_float(std::numeric_limits<double>::quiet_NaN(), asc)));
}

TEST_F(coder_float_test, inf) {
    EXPECT_EQ("ff800000", to_hex(write_float(std::numeric_limits<float>::infinity(), asc)));
    EXPECT_EQ("007fffff", to_hex(write_float(-std::numeric_limits<float>::infinity(), asc)));
    EXPECT_EQ("fff0000000000000", to_hex(write_float(std::numeric_limits<double>::infinity(), asc)));
    EXPECT_EQ("000fffffffffffff", to_hex(write_float(-std::numeric_limits<double>::infinity(), asc)));
}

// TODO normalize negative zero
TEST_F(coder_float_test, zeros) {
    EXPECT_EQ("80000000", to_hex(write_float(0.0F, asc)));
    // EXPECT_EQ("80000000", to_hex(write_float(-0.0F, asc)));
    EXPECT_EQ("8000000000000000", to_hex(write_float(0.0, asc)));
    // EXPECT_EQ("8000000000000000", to_hex(write_float(-0.0, asc)));
}

TEST_F(coder_float_test, order_float4) {
    auto denorm_max = std::nextafterf(std::numeric_limits<float>::min(), 0.0F);
    EXPECT_LT(denorm_max, std::numeric_limits<float>::min());
    EXPECT_LT(std::numeric_limits<float>::denorm_min(), denorm_max);
    EXPECT_FALSE(std::isnormal(denorm_max));

    auto nan = write_float(std::numeric_limits<float>::quiet_NaN(), asc); EXPECT_EQ(4, nan.size());
    auto positive_inf = write_float(std::numeric_limits<float>::infinity(), asc); EXPECT_EQ(4, positive_inf.size());
    EXPECT_LT(positive_inf, nan);
    auto positive_normal_max = write_float(std::numeric_limits<float>::max(), asc); EXPECT_EQ(4, positive_normal_max.size());
    EXPECT_LT(positive_normal_max, positive_inf);
    auto positive_normal_min = write_float(std::numeric_limits<float>::min(), asc); EXPECT_EQ(4, positive_normal_min.size());
    EXPECT_LT(positive_normal_min, positive_normal_max);
    auto positive_subnormal_max = write_float(denorm_max , asc); EXPECT_EQ(4, positive_subnormal_max.size());
    EXPECT_LT(positive_subnormal_max, positive_normal_min);
    auto positive_subnormal_min = write_float(std::numeric_limits<float>::denorm_min(), asc); EXPECT_EQ(4, positive_subnormal_min.size());
    EXPECT_LT(positive_subnormal_min, positive_subnormal_max);
    auto zero = write_float(0.0F, asc); EXPECT_EQ(4, zero.size());
    EXPECT_LT(zero, positive_subnormal_min);
    auto negative_subnormal_min = write_float(-std::numeric_limits<float>::denorm_min(), asc); EXPECT_EQ(4, negative_subnormal_min.size());
    EXPECT_LT(negative_subnormal_min, zero);
    auto negative_subnormal_max = write_float(-denorm_max , asc); EXPECT_EQ(4, negative_subnormal_max.size());
    EXPECT_LT(negative_subnormal_max, negative_subnormal_min);
    auto negative_normal_min = write_float(-std::numeric_limits<float>::min(), asc); EXPECT_EQ(4, negative_normal_min.size());
    EXPECT_LT(negative_normal_min, negative_subnormal_max);
    auto negative_normal_max = write_float(-std::numeric_limits<float>::max(), asc); EXPECT_EQ(4, negative_normal_max.size());
    EXPECT_LT(negative_normal_max, negative_normal_min);
    auto negative_inf = write_float(-std::numeric_limits<float>::infinity(), asc); EXPECT_EQ(4, negative_inf.size());
    EXPECT_LT(negative_inf, negative_normal_max);
}

TEST_F(coder_float_test, order_float8) {
    auto denorm_max = std::nextafter(std::numeric_limits<double>::min(), 0.0);
    EXPECT_LT(denorm_max, std::numeric_limits<double>::min());
    EXPECT_LT(std::numeric_limits<double>::denorm_min(), denorm_max);
    EXPECT_FALSE(std::isnormal(denorm_max));

    auto nan = write_float(std::numeric_limits<double>::quiet_NaN(), asc); EXPECT_EQ(8, nan.size());
    auto positive_inf = write_float(std::numeric_limits<double>::infinity(), asc); EXPECT_EQ(8, positive_inf.size());
    EXPECT_LT(positive_inf, nan);
    auto positive_normal_max = write_float(std::numeric_limits<double>::max(), asc); EXPECT_EQ(8, positive_normal_max.size());
    EXPECT_LT(positive_normal_max, positive_inf);
    auto positive_normal_min = write_float(std::numeric_limits<double>::min(), asc); EXPECT_EQ(8, positive_normal_min.size());
    EXPECT_LT(positive_normal_min, positive_normal_max);
    auto positive_subnormal_max = write_float(denorm_max , asc); EXPECT_EQ(8, positive_subnormal_max.size());
    EXPECT_LT(positive_subnormal_max, positive_normal_min);
    auto positive_subnormal_min = write_float(std::numeric_limits<double>::denorm_min(), asc); EXPECT_EQ(8, positive_subnormal_min.size());
    EXPECT_LT(positive_subnormal_min, positive_subnormal_max);
    auto zero = write_float(0.0, asc); EXPECT_EQ(8, zero.size());
    EXPECT_LT(zero, positive_subnormal_min);
    auto negative_subnormal_min = write_float(-std::numeric_limits<double>::denorm_min(), asc); EXPECT_EQ(8, negative_subnormal_min.size());
    EXPECT_LT(negative_subnormal_min, zero);
    auto negative_subnormal_max = write_float(-denorm_max , asc); EXPECT_EQ(8, negative_subnormal_max.size());
    EXPECT_LT(negative_subnormal_max, negative_subnormal_min);
    auto negative_normal_min = write_float(-std::numeric_limits<double>::min(), asc); EXPECT_EQ(8, negative_normal_min.size());
    EXPECT_LT(negative_normal_min, negative_subnormal_max);
    auto negative_normal_max = write_float(-std::numeric_limits<double>::max(), asc); EXPECT_EQ(8, negative_normal_max.size());
    EXPECT_LT(negative_normal_max, negative_normal_min);
    auto negative_inf = write_float(-std::numeric_limits<double>::infinity(), asc); EXPECT_EQ(8, negative_inf.size());
    EXPECT_LT(negative_inf, negative_normal_max);
}

}
