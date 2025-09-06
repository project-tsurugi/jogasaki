/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <jogasaki/executor/common_column_utils.h>

#include <gtest/gtest.h>

#include <jogasaki/proto/sql/common.pb.h>

using jogasaki::proto::sql::common::Column;

namespace jogasaki::executor {

namespace proto = jogasaki::proto;

class common_column_utils_test : public ::testing::Test {
protected:
};

TEST(common_column_utils_test, proto_to_common_and_back_roundtrip) {
    Column src{};
    src.set_name("col1");
    src.set_atom_type(proto::sql::common::AtomType::INT4);
    src.set_dimension(3);
    src.set_length(42);
    src.set_precision(10);
    src.mutable_arbitrary_scale();
    src.set_nullable(true);
    src.set_varying(false);
    src.set_description("desc1");

    // from proto -> common_column
    auto common = from_proto(src);

    // convert back
    auto out = to_proto(common);

    EXPECT_EQ(out.name(), src.name());
    EXPECT_EQ(out.atom_type(), src.atom_type());
    EXPECT_EQ(out.dimension(), src.dimension());
    EXPECT_EQ(out.length(), src.length());
    EXPECT_EQ(out.precision(), src.precision());
    EXPECT_TRUE(has_arbitrary_scale(out));
    EXPECT_EQ(out.nullable(), src.nullable());
    EXPECT_EQ(out.varying(), src.varying());
    EXPECT_EQ(out.description(), src.description());
}

TEST(common_column_utils_test, common_to_proto_and_back_roundtrip) {
    common_column c{};
    c.name_ = "col2";
    c.atom_type_ = common_column::atom_type::decimal;
    c.dimension_ = 1;
    c.length_ = std::variant<std::uint32_t, bool>{static_cast<std::uint32_t>(128)};
    c.precision_ = std::variant<std::uint32_t, bool>{true}; // arbitrary precision
    c.scale_ = std::variant<std::uint32_t, bool>{static_cast<std::uint32_t>(2)};
    c.nullable_ = false;
    c.varying_ = true;
    c.description_ = std::string("desc2");

    auto proto = to_proto(c);
    auto round = from_proto(proto);

    EXPECT_EQ(round, c);
}

TEST(common_column_utils_test, proto_default_roundtrip) {
    Column src{};

    // from proto -> common_column
    auto common = from_proto(src);

    // convert back
    auto out = to_proto(common);

    EXPECT_EQ(out.name(), src.name());
    EXPECT_EQ(out.atom_type(), src.atom_type());
    EXPECT_EQ(out.dimension(), src.dimension());
    EXPECT_TRUE(! has_length(out));
    EXPECT_TRUE(! has_arbitrary_length(out));
    EXPECT_TRUE(! has_precision(out));
    EXPECT_TRUE(! has_arbitrary_precision(out));
    EXPECT_TRUE(! has_scale(out));
    EXPECT_TRUE(! has_arbitrary_scale(out));
    EXPECT_TRUE(! has_nullable(out));
    EXPECT_TRUE(! has_varying(out));
    EXPECT_TRUE(out.description().empty());
}
TEST(common_column_utils_test, common_default_roundtrip) {
    common_column c{};

    auto proto = jogasaki::executor::to_proto(c);
    auto round = jogasaki::executor::from_proto(proto);

    EXPECT_EQ(round, c);
}

} // namespace jogasaki::executor
