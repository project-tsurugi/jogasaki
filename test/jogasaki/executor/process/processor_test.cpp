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

#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/executor/process/mock/process_executor.h>

namespace jogasaki::executor::process::impl {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class processor_test : public test_root {};

TEST_F(processor_test, basic) {
    using kind = meta::field_type_kind;
    auto meta = jogasaki::mock::create_meta<kind::int8>();
    jogasaki::executor::process::mock::basic_record_writer::records_type records{};
    auto reader = jogasaki::executor::process::mock::create_reader_shared<kind::int8>(records);
    reader_container r{reader.get()};
    auto downstream_writer = mock::create_writer_shared<kind::int8>();
    auto external_writer = mock::create_writer_shared<kind::int8>();
    auto context = std::make_shared<mock::task_context>(
        std::vector<reader_container>{r},
        std::vector<std::shared_ptr<executor::record_writer>>{downstream_writer},
        std::vector<std::shared_ptr<executor::record_writer>>{external_writer},
        std::shared_ptr<abstract::scan_info>{}
    );
    auto proc = std::make_shared<processor>();
//    proc->run(context.get());
}

}

