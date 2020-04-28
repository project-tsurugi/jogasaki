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

#include <vector>
#include <gtest/gtest.h>
#include <executor/group_reader.h>
#include <key_count.h>
#include <value.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/rvalue_reference_wrapper.h>

namespace dc::testing {
using namespace std::literals::string_literals;
using namespace data;
using namespace executor;

class reader_interface_test : public ::testing::Test {
public:
};

class mock_group_reader : public group_reader {
private:
    std::vector<key_count> keys_;
    std::vector<value> values_;
    decltype(keys_)::iterator key_position_;
    decltype(values_)::iterator value_position_;
    std::size_t current_member_offset_{std::size_t(-1)};
    std::size_t current_group_members_count_{};
    bool initial_position = true;

    constexpr static std::size_t npos = std::size_t(-1);
public:
    mock_group_reader(std::vector<key_count> keys, std::vector<value> values) :
            keys_(std::move(keys)), values_(std::move(values)),
            key_position_(keys_.begin()), value_position_(values_.begin()) {}

    bool next_group() override {
        if (initial_position) {
            initial_position = false;
        } else {
            ++key_position_;
        }
        if (key_position_ == keys_.end()) {
            return false;
        }
        value_position_ += current_group_members_count_;
        current_group_members_count_ = key_position_->count();
        current_member_offset_ = npos;
        return true;
    }

    accessor::record_ref get_group() const override {
        if (initial_position) return accessor::record_ref();
        accessor::record_ref ret{&*key_position_, sizeof(key_count)};
        return ret;
    }

    bool next_member() override {
        if (initial_position) return false; // invalid
        if (current_member_offset_ == npos) {
            current_member_offset_ = 0;
        } else {
            ++current_member_offset_;
        }
        if (current_member_offset_ < current_group_members_count_) {
            return true;
        }
        return false;
    }

    accessor::record_ref get_member() const override {
        if (initial_position) return accessor::record_ref();
        if (current_member_offset_ == npos) return accessor::record_ref();
        if (current_member_offset_ < current_group_members_count_) {
            accessor::record_ref ret{ &*(value_position_+current_member_offset_), sizeof(value)};
            return ret;
        }
        return accessor::record_ref();
    }
    void release() override {}
};

TEST_F(reader_interface_test, simple) {

    mock_group_reader reader{std::vector<key_count>{
            key_count{1, 2},
            key_count{2, 2},
            key_count{3, 1},
    }, std::vector<value>{
            value(10.0),
            value(11.0),

            value(20.0),
            value(21.0),

            value(30.0)}};
    EXPECT_TRUE(reader.next_group());
    auto k = reader.get_group();
    EXPECT_EQ(1, k.get_value<key_count::key_type>(0));
    EXPECT_TRUE(reader.next_member());
    auto v = reader.get_member();
    EXPECT_DOUBLE_EQ(10.0, v.get_value<value::value_type>(0));
    EXPECT_TRUE(reader.next_member());
    v = reader.get_member();
    EXPECT_DOUBLE_EQ(11.0, v.get_value<value::value_type>(0));
    EXPECT_FALSE(reader.next_member());

    EXPECT_TRUE(reader.next_group());
    k = reader.get_group();
    EXPECT_EQ(2, k.get_value<key_count::key_type>(0));
    EXPECT_TRUE(reader.next_member());
    v = reader.get_member();
    EXPECT_DOUBLE_EQ(20.0, v.get_value<value::value_type>(0));
    EXPECT_TRUE(reader.next_member());
    v = reader.get_member();
    EXPECT_DOUBLE_EQ(21.0, v.get_value<value::value_type>(0));
    EXPECT_FALSE(reader.next_member());

    EXPECT_TRUE(reader.next_group());
    k = reader.get_group();
    EXPECT_EQ(3, k.get_value<key_count::key_type>(0));
    EXPECT_TRUE(reader.next_member());
    v = reader.get_member();
    EXPECT_DOUBLE_EQ(30.0, v.get_value<value::value_type>(0));
    EXPECT_FALSE(reader.next_member());

}

}
