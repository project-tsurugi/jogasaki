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
#include <jogasaki/executor/process/process_executor.h>

#include <string>

#include <takatori/util/object_creator.h>
#include <gtest/gtest.h>

#include <jogasaki/test_root.h>

#include "mock/task_context.h"
#include "mock/process_executor.h"
#include "mock/processor.h"

namespace jogasaki::executor::process {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace testing;
using namespace jogasaki::memory;
using namespace boost::container::pmr;

class process_executor_test : public test_root {};

using kind = meta::field_type_kind;

TEST_F(process_executor_test, basic) {
    using record_type = mock::record_reader::record_type;
    std::vector<record_type> records{
        record_type{1, 1.0},
        record_type{2, 2.0},
        record_type{3, 3.0},
    };
    auto reader = std::make_shared<mock::record_reader>(records);
    reader_container r{reader.get()};
    auto meta = unwrap_record_reader(reader.get())->meta();
    auto downstream_writer = std::make_shared<mock::record_writer>();
    auto external_writer = std::make_shared<mock::record_writer>();
    auto context = std::make_shared<mock::task_context>(r, downstream_writer, external_writer);
    auto proc = std::make_shared<mock::processor>();

    mock::process_executor exec{proc, context};
    exec.run();

    auto written = unwrap_record_writer(downstream_writer.get())->size();
    auto written2 = unwrap_record_writer(external_writer.get())->size();
    EXPECT_EQ(3, written);
    EXPECT_EQ(3, written2);

//    EXPECT_TRUE(reader->released_);
//    EXPECT_TRUE(downstream_writer->released_);
//    EXPECT_TRUE(external_writer->released_);
}

}

