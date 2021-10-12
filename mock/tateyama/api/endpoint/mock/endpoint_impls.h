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
#include <tateyama/api/endpoint/writer.h>
#include <tateyama/api/endpoint/request.h>
#include <tateyama/api/endpoint/response.h>
#include <tateyama/api/endpoint/data_channel.h>

#include <memory>
#include <regex>

namespace tateyama::api::endpoint::mock {

using namespace std::literals::string_literals;
using namespace std::string_view_literals;

class test_writer : public writer {

public:
    test_writer() = default;

    test_writer(char* data, std::size_t capacity);

    status write(char const* data, std::size_t length) override;

    status commit() override;

    char* data_{};  //NOLINT
    std::size_t capacity_{};  //NOLINT
    std::size_t size_{};  //NOLINT
};

template<std::size_t Size>
class fixed_buffer_writer : public test_writer {
public:
    fixed_buffer_writer() {
        data_ = array_.data();
        capacity_ = array_.size();
    }

private:
    std::array<char, Size> array_{};
};

class test_request : public request {
public:
    test_request() = default;

    explicit test_request(std::string_view payload);

    [[nodiscard]] std::string_view payload() const override;

    std::string payload_{};  //NOLINT
};

class test_channel : public data_channel {
public:
    test_channel() = default;

    status acquire(writer*& buf) override;

    status release(writer& buf) override;

    [[nodiscard]] bool all_released() const noexcept;

    std::vector<std::shared_ptr<test_writer>> buffers_{};  //NOLINT

    std::size_t released_{};  //NOLINT
};

class test_response : public response {
public:

    void code(response_code code) override;

    status body(std::string_view body) override;

    status body_head(std::string_view body_head) override;

    status acquire_channel(std::string_view name, data_channel*& ch) override;

    status release_channel(data_channel& ch) override;

    bool completed();

    [[nodiscard]] bool all_released() const noexcept;

    status close_session() override;;
    std::string body_{};  //NOLINT
    std::string body_head_{};  //NOLINT
    std::unique_ptr<test_channel> channel_{};  //NOLINT
    std::string message_{};  //NOLINT
    response_code code_{response_code::unknown};  //NOLINT
    bool completed_{};  //NOLINT
    std::size_t released_{};  //NOLINT
};

}
