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
#include <jogasaki/executor/diagnostic_record.h>

#include <vector>

#include <jogasaki/test_root.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>

namespace jogasaki::executor {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace jogasaki::executor::process::impl::expression;

class diagnostic_record_test : public test_root {
public:
};

TEST_F(diagnostic_record_test, simple) {
    diagnostic_record<error_kind> rec{error_kind::lost_precision, "simple"};
    std::stringstream ss{};
    ss << rec;
    EXPECT_EQ("diagnostic(code=lost_precision, message='simple')", ss.str());
}

TEST_F(diagnostic_record_test, args) {
    diagnostic_record<error_kind> rec{error_kind::lost_precision, "arguments"};
    rec.new_argument() << 0;
    rec.new_argument() << 1.0;
    rec.new_argument() << "2";
    std::stringstream ss{};
    ss << rec;
    EXPECT_EQ("diagnostic(code=lost_precision, message='arguments', args=['0', '1', '2'])", ss.str());
}
}  // namespace jogasaki::executor
