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
#include <jogasaki/executor/process/impl/processor.h>

#include <string>

#include <takatori/util/object_creator.h>
#include <gtest/gtest.h>

#include <jogasaki/test_root.h>

#include "mock/task_context.h"
#include "mock/process_executor.h"
#include "mock/processor.h"

namespace jogasaki::executor::process::impl {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class processor_impl_test : public test_root {};

TEST_F(processor_impl_test, basic) {
    auto reader = std::make_shared<mock::record_reader>();
    reader_container r{reader.get()};
    auto downstream_writer = std::make_shared<mock::record_writer>(test_record_meta1());
    auto external_writer = std::make_shared<mock::external_writer>(test_record_meta1());
    auto context = std::make_shared<mock::task_context>(r, downstream_writer, external_writer);

//    auto proc = std::make_shared<processor_impl>();

//    mock::process_executor exec{proc, context};

//    exec.run();

//    auto& written = downstream_writer->records_;
//    auto& written2 = external_writer->records_;
//    EXPECT_EQ(3, written.size());
//    EXPECT_EQ(3, written2.size());
//
//    EXPECT_TRUE(reader->released_);
//    EXPECT_TRUE(downstream_writer->released_);
//    EXPECT_TRUE(external_writer->released_);
}

}

