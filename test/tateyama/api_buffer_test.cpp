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
#include <tateyama/api/buffer.h>

#include <regex>
#include <gtest/gtest.h>

namespace tateyama::api {

using namespace std::literals::string_literals;

class api_buffer_test : public ::testing::Test {

};

using namespace std::string_view_literals;

class my_buffer : public buffer {
public:
    my_buffer(std::byte* data, std::size_t capacity) :
        data_(data), capacity_(capacity)
    {}

    virtual std::byte* data() override {
        return data_;
    }

    std::size_t capacity() override {
        return capacity_;
    }

    void size(std::size_t sz) override {
        size_ = sz;
    }
private:
    std::byte* data_{};
    std::size_t capacity_{};
    std::size_t size_{};
};

TEST_F(api_buffer_test, basic) {
    std::array<std::byte, 100> s{};
    my_buffer buf{s.data(), s.size()};
}

}
