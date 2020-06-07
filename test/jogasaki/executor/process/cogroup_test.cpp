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
#include <jogasaki/executor/process/cogroup.h>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <jogasaki/test_utils.h>
#include <jogasaki/test_root.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

#include <jogasaki/executor/process/join.h>

#include "mock/group_reader.h"

namespace jogasaki::executor::process {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

namespace type = ::takatori::type;
namespace value = ::takatori::value;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace statement = ::takatori::statement;

class cogroup_test : public test_root {

public:
    using kind = field_type_kind;

};

TEST_F(cogroup_test, simple) {
    mock::group_reader r1 {
            {
                    mock::group_entry{
                            1,
                            mock::group_entry::value_type{
                                    100.0,
                                    101.0,
                            },
                    },
                    mock::group_entry{
                            2,
                            mock::group_entry::value_type{
                                    200.0,
                                    201.0,
                            },
                    }
            }
    };
    auto r2 = r1;
    cogroup cgrp{
        std::vector<group_reader*>{&r1, &r2},
        std::vector<std::shared_ptr<meta::group_meta>>{test_group_meta1(), test_group_meta1()}
    };
    join j{};
    auto f = std::function(j);
//    cgrp(f);

}

}

