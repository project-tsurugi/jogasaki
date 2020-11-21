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
#include <jogasaki/executor/process/impl/ops/take_cogroup.h>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <jogasaki/test_root.h>

#include <jogasaki/executor/process/impl/ops/join.h>

#include <jogasaki/executor/process/mock/group_reader.h>

namespace jogasaki::executor::process::impl::ops {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class take_cogroup_test : public test_root {

public:
    using kind = field_type_kind;

};

using group_type = mock::group_reader::group_type;
using keys_type = group_type::key_type;
using values_type = group_type::value_type;

}

