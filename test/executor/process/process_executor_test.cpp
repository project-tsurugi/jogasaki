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
#include <executor/process/process_executor.h>

#include <string>
#include <string_view>

#include <takatori/util/object_creator.h>
#include <gtest/gtest.h>
#include <accessor/record_ref.h>

#include <mock_memory_resource.h>
#include <memory/monotonic_paged_memory_resource.h>
#include "test_root.h"

namespace jogasaki::executor::process {

//using namespace data;
using namespace executor;
//using namespace meta;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class process_executor_test : public test_root {};

TEST_F(process_executor_test, basic) {
    process_executor exec{};
}

}

