/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <memory>

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <jogasaki/check_cxx_std.h>
#include <jogasaki/executor/function/function_registry.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/kvs/environment.h>
#include <jogasaki/logging.h>

int main(int argc, char** argv) {
    // first consume command line options for gtest
    ::testing::InitGoogleTest(&argc, argv);
    FLAGS_logtostderr = true;
    FLAGS_v = FLAGS_v < jogasaki::log_info ? jogasaki::log_info : FLAGS_v;
    jogasaki::kvs::environment env{};
    env.initialize();
    auto functions = std::make_shared<jogasaki::executor::function::function_registry>();
    (void) jogasaki::global::function_registry(functions);
    return RUN_ALL_TESTS();
}
