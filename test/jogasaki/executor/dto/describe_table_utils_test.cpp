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
#include <jogasaki/executor/dto/describe_table_utils.h>

#include <gtest/gtest.h>

#include <takatori/util/string_builder.h>

#include <jogasaki/proto/sql/response.pb.h>

using jogasaki::proto::sql::response::DescribeTable;

namespace jogasaki::executor::dto {

class describe_table_utils_test : public ::testing::Test {
protected:
};


TEST(describe_table_utils_test, proto_to_common_and_back_roundtrip) {
    DescribeTable::Success src{};
    src.set_database_name("db1");
    src.set_schema_name("public");
    src.set_table_name("t1");

    // add one column using proto common Column
    auto* col = src.add_columns();
    col->set_name("c1");
    col->set_atom_type(jogasaki::proto::sql::common::AtomType::INT4);
    col->set_dimension(0);

    src.add_primary_key("c1");
    src.set_description("table desc");

    auto common = from_proto(src);
    auto out = to_proto(common);

    EXPECT_EQ(out.database_name(), src.database_name());
    EXPECT_EQ(out.schema_name(), src.schema_name());
    EXPECT_EQ(out.table_name(), src.table_name());
    ASSERT_EQ(out.columns_size(), src.columns_size());
    EXPECT_EQ(out.primary_key(0), src.primary_key(0));
    EXPECT_EQ(out.description(), src.description());
}

TEST(describe_table_utils_test, common_to_proto_and_back_roundtrip) {
    describe_table dt{};
    dt.database_name_ = "db2";
    dt.schema_name_ = "s2";
    dt.table_name_ = "t2";
    common_column c{};
    c.name_ = "c2";
    c.atom_type_ = common_column::atom_type::character;
    dt.columns_.emplace_back(c);
    dt.primary_key_.emplace_back(std::string("c2"));
    dt.description_ = std::string("desc2");

    auto proto = to_proto(dt);
    auto round = from_proto(proto);

    EXPECT_EQ(round, dt);
}

TEST(describe_table_utils_test, to_string) {
    describe_table dt{};
    dt.table_name_ = "t";
    common_column c0{};
    c0.name_ = "c0";
    c0.atom_type_ = common_column::atom_type::int4;
    dt.columns_.emplace_back(c0);
    common_column c1{};
    c1.name_ = "c1";
    c1.atom_type_ = common_column::atom_type::int4;
    dt.columns_.emplace_back(c1);
    common_column c2{};
    c2.name_ = "c2";
    c2.atom_type_ = common_column::atom_type::int4;
    dt.columns_.emplace_back(c2);
    dt.primary_key_.emplace_back(std::string("c1"));
    dt.primary_key_.emplace_back(std::string("c2"));
    dt.description_ = std::string("desc0");

    auto str = takatori::util::string_builder{} << dt << takatori::util::string_builder::to_string;
    EXPECT_EQ("describe_table{name:\"t\" desc:\"desc0\" columns:[common_column{name:\"c0\" type:int4},common_column{name:\"c1\" type:int4},common_column{name:\"c2\" type:int4}] pk:[\"c1\",\"c2\"]}", str);
}

} // namespace jogasaki::executor
