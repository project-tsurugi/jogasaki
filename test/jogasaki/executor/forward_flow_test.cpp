/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <gtest/gtest.h>

#include <jogasaki/executor/exchange/forward/flow.h>
#include <jogasaki/executor/exchange/forward/forward_info.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/io/record_reader.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>

namespace jogasaki::executor::exchange::forward {

using kind = meta::field_type_kind;

/**
 * @brief tests for the row count of forward exchange flow
 */
class forward_flow_test : public ::testing::Test {
public:
    // writes `rows` records to each sink of the flow, and returns the number of records passed to the downstream
    static std::size_t write_and_count(flow& f, std::size_t rows) {
        auto rec = mock::create_nullable_record<kind::int4>(1);
        for(std::size_t i = 0; i < f.sink_count(); ++i) {
            auto& w = f.sink_at(i).acquire_writer();
            for(std::size_t j = 0; j < rows; ++j) {
                (void) w.write(rec.ref());
            }
            w.flush();
            w.release();
        }
        std::size_t ret = 0;
        for(std::size_t i = 0; i < f.source_count(); ++i) {
            auto* r = f.source_at(i).acquire_reader().reader<io::record_reader>();
            while(r->next_record()) {
                ++ret;
            }
        }
        return ret;
    }

    static std::shared_ptr<forward_info> create_info(std::size_t limit) {
        return std::make_shared<forward_info>(mock::create_nullable_record<kind::int4>(0).record_meta(), limit);
    }
};

TEST_F(forward_flow_test, limit_over_partitions) {
    // the row count is for the whole input, not for each partition
    flow f{create_info(3), nullptr, nullptr};
    f.setup_partitions(2);
    ASSERT_EQ(2, f.sink_count());
    EXPECT_EQ(3, write_and_count(f, 5));
}

TEST_F(forward_flow_test, limit_over_partitions_set_up_separately) {
    // setup_partitions() is called for each upstream process (or each offer operator) writing to the exchange
    flow f{create_info(3), nullptr, nullptr};
    f.setup_partitions(1);
    f.setup_partitions(1);
    ASSERT_EQ(2, f.sink_count());
    EXPECT_EQ(3, write_and_count(f, 5));
}

}  // namespace jogasaki::executor::exchange::forward
