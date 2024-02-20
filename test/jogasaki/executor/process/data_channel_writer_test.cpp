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
#include <jogasaki/executor/io/data_channel_writer.h>

#include <string>

#include <gtest/gtest.h>

#include <jogasaki/executor/io/record_channel_adapter.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/mock/test_channel.h>
#include <jogasaki/utils/msgbuf_utils.h>
#include <jogasaki/test_utils/types.h>

namespace jogasaki::executor::io {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;
using namespace std::chrono_literals;

using namespace jogasaki::memory;
using namespace jogasaki::mock;
using namespace boost::container::pmr;

class data_channel_writer_test : public ::testing::Test {};

TEST_F(data_channel_writer_test, basic) {
    using kind = meta::field_type_kind;
    auto meta = create_meta<kind::int4, kind::float8, kind::int8, kind::float4, kind::character>();

    api::test_channel ch{};
    executor::io::record_channel_adapter record_ch{maybe_shared_ptr<api::data_channel>{&ch}};
    std::shared_ptr<api::writer> wr{};
    ASSERT_EQ(status::ok, ch.acquire(wr));
    data_channel_writer writer{record_ch, std::move(wr), meta};

    auto rec1 = create_record<kind::int4, kind::float8, kind::int8, kind::float4, kind::character>(1, 10.0, 100, 1000.0, accessor::text{"111"});
    auto rec2 = create_record<kind::int4, kind::float8, kind::int8, kind::float4, kind::character>(2, 20.0, 200, 2000.0, accessor::text{"222"});
    auto rec3 = create_record<kind::int4, kind::float8, kind::int8, kind::float4, kind::character>(3, 30.0, 300, 3000.0, accessor::text{"333"});
    writer.write(rec1.ref());
    writer.write(rec2.ref());
    writer.write(rec3.ref());
    writer.flush();

    ASSERT_EQ(1, ch.writers_.size());
    auto& w = *ch.writers_[0];
    auto recs = utils::deserialize_msg({w.data_.data(), w.size_}, *meta);
    ASSERT_EQ(3, recs.size());
    EXPECT_EQ(rec1, recs[0]);
    EXPECT_EQ(rec2, recs[1]);
    EXPECT_EQ(rec3, recs[2]);
}

TEST_F(data_channel_writer_test, temporal_types) {
    using kind = meta::field_type_kind;
    auto meta = create_meta<kind::int4, kind::date, kind::time_of_day, kind::time_point>();

    api::test_channel ch{};
    executor::io::record_channel_adapter record_ch{maybe_shared_ptr<api::data_channel>{&ch}};
    std::shared_ptr<api::writer> wr{};
    ASSERT_EQ(status::ok, ch.acquire(wr));
    data_channel_writer writer{record_ch, std::move(wr), meta};

    auto rec1 = create_record<kind::int4, kind::date, kind::time_of_day, kind::time_point>(1, rtype<ft::date>{10}, rtype<ft::time_of_day>{100ns}, rtype<ft::time_point>{1000ns});
    auto rec2 = create_record<kind::int4, kind::date, kind::time_of_day, kind::time_point>(2, rtype<ft::date>{20}, rtype<ft::time_of_day>{200ns}, rtype<ft::time_point>{2000ns});
    auto rec3 = create_record<kind::int4, kind::date, kind::time_of_day, kind::time_point>(2, rtype<ft::date>{30}, rtype<ft::time_of_day>{300ns}, rtype<ft::time_point>{3000ns});
    writer.write(rec1.ref());
    writer.write(rec2.ref());
    writer.write(rec3.ref());
    writer.flush();

    ASSERT_EQ(1, ch.writers_.size());
    auto& w = *ch.writers_[0];
    auto recs = utils::deserialize_msg({w.data_.data(), w.size_}, *meta);
    ASSERT_EQ(3, recs.size());
    EXPECT_EQ(rec1, recs[0]);
    EXPECT_EQ(rec2, recs[1]);
    EXPECT_EQ(rec3, recs[2]);
}
}

