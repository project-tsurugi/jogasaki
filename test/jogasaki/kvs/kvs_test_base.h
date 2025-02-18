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

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/test_base.h>
#include <jogasaki/test_utils/temporary_folder.h>

namespace jogasaki::kvs {

class kvs_test_base : public test_base {
public:
    static constexpr std::string_view KEY_LOCATION{"location"};

    void db_setup(std::map<std::string, std::string> opts = {});

    void db_teardown();

    [[nodiscard]] std::string path() const;

    test::temporary_folder temporary_{};  //NOLINT
    std::shared_ptr<database> db_{};  //NOLINT

};

}
