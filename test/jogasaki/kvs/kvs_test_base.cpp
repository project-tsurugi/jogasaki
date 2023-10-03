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
#include "kvs_test_base.h"

#include <string_view>
#include <string>
#include <map>
#include <memory>
#include <gtest/gtest.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/test_base.h>
#include <jogasaki/test_utils/temporary_folder.h>

namespace jogasaki::kvs {

void kvs_test_base::db_setup(std::map<std::string, std::string> opts) {
    temporary_.prepare();

    if (opts.count(std::string{KEY_LOCATION}) == 0) {
        opts.emplace(KEY_LOCATION, path());
    }
    db_ = kvs::database::open(opts);
}

void kvs_test_base::db_teardown() {
    (void)db_->close();
    temporary_.clean();
}

std::string kvs_test_base::path() const {
    return temporary_.path();
}
}
