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
#pragma once

#include <gtest/gtest.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/fail.h>

#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_utils/record.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/temporary_folder.h>
#include <jogasaki/kvs_test_utils.h>
#include <jogasaki/test_base.h>

namespace jogasaki {

using takatori::util::maybe_shared_ptr;
using takatori::util::fail;
using kind = meta::field_type_kind;

class kvs_test_base :
    public kvs_test_utils,
    public test_base {
public:

    void kvs_db_setup() {
        temporary_.prepare();
        using namespace std::string_literals;
        std::map<std::string, std::string> m{{"location"s, path()}};
        db_ = kvs::database::open(m);
        if (! db_) fail();
    }

    void kvs_db_teardown() {
        if(auto rc = db_->close(); ! rc) {
            fail();
        }
        temporary_.clean();
    }

    [[nodiscard]] std::string path() const {
        return temporary_.path();
    }
    test::temporary_folder temporary_{};  //NOLINT
    std::shared_ptr<kvs::database> db_;  //NOLINT
};

}
