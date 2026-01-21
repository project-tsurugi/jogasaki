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
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <jogasaki/kvs/environment.h>
#include <jogasaki/logging.h>

#include <iostream>
#include <string>

/**
 * @brief Custom test event listener to capture stdout/stderr and show only on failure
 *
 * This listener reduces log output by capturing stdout and stderr during test execution
 * and only displaying them when a test fails. This helps reduce CI log volume while
 * preserving diagnostic information for failed tests.
 */
class capture_output_listener : public ::testing::TestEventListener {
public:
    explicit capture_output_listener(::testing::TestEventListener* listener)
        : listener_(listener) {}

    ~capture_output_listener() override {
        delete listener_;
    }

    void OnTestProgramStart(const ::testing::UnitTest& unit_test) override {
        listener_->OnTestProgramStart(unit_test);
    }

    void OnTestIterationStart(const ::testing::UnitTest& unit_test, int iteration) override {
        listener_->OnTestIterationStart(unit_test, iteration);
    }

    void OnEnvironmentsSetUpStart(const ::testing::UnitTest& unit_test) override {
        listener_->OnEnvironmentsSetUpStart(unit_test);
    }

    void OnEnvironmentsSetUpEnd(const ::testing::UnitTest& unit_test) override {
        listener_->OnEnvironmentsSetUpEnd(unit_test);
    }

    void OnTestSuiteStart(const ::testing::TestSuite& test_suite) override {
        listener_->OnTestSuiteStart(test_suite);
    }

    void OnTestStart(const ::testing::TestInfo& test_info) override {
        listener_->OnTestStart(test_info);
        // Start capturing stdout and stderr
        ::testing::internal::CaptureStdout();
        ::testing::internal::CaptureStderr();
    }

    void OnTestPartResult(const ::testing::TestPartResult& test_part_result) override {
        listener_->OnTestPartResult(test_part_result);
    }

    void OnTestEnd(const ::testing::TestInfo& test_info) override {
        // Retrieve captured output
        std::string const out = ::testing::internal::GetCapturedStdout();
        std::string const err = ::testing::internal::GetCapturedStderr();

        // If test failed, display captured output
        if (test_info.result()->Failed()) {
            if (! out.empty()) {
                std::cerr << "\n[Captured stdout]\n" << out;
            }
            if (! err.empty()) {
                std::cerr << "\n[Captured stderr]\n" << err;
            }
        }

        listener_->OnTestEnd(test_info);
    }

    void OnTestSuiteEnd(const ::testing::TestSuite& test_suite) override {
        listener_->OnTestSuiteEnd(test_suite);
    }

    void OnEnvironmentsTearDownStart(const ::testing::UnitTest& unit_test) override {
        listener_->OnEnvironmentsTearDownStart(unit_test);
    }

    void OnEnvironmentsTearDownEnd(const ::testing::UnitTest& unit_test) override {
        listener_->OnEnvironmentsTearDownEnd(unit_test);
    }

    void OnTestIterationEnd(const ::testing::UnitTest& unit_test, int iteration) override {
        listener_->OnTestIterationEnd(unit_test, iteration);
    }

    void OnTestProgramEnd(const ::testing::UnitTest& unit_test) override {
        listener_->OnTestProgramEnd(unit_test);
    }

private:
    ::testing::TestEventListener* listener_;
};

int main(int argc, char** argv) {
    // first consume command line options for gtest
    ::testing::InitGoogleTest(&argc, argv);
    FLAGS_logtostderr = true;
    FLAGS_v = FLAGS_v < jogasaki::log_info ? jogasaki::log_info : FLAGS_v;

    // Install custom listener to capture output and show only on failure
    ::testing::TestEventListeners& listeners = ::testing::UnitTest::GetInstance()->listeners();
    ::testing::TestEventListener* default_printer = listeners.Release(listeners.default_result_printer());
    listeners.Append(new capture_output_listener(default_printer));

    jogasaki::kvs::environment env{};
    env.initialize();
    return RUN_ALL_TESTS();
}
