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
#include <tateyama/api/endpoint/provider.h>
#include <tateyama/api/registry.h>

#include <memory>
#include <regex>
#include <atomic>

namespace tateyama::api::endpoint::mock {

using namespace std::literals::string_literals;
using namespace std::string_view_literals;

class test_writer : public writer {

public:
    test_writer() = default;

    test_writer(char* data, std::size_t capacity);

    status write(char const* data, std::size_t length) override;

    status commit() override;

    std::string_view read();

    void set_on_write(std::function<void(std::string_view)> on_write);

    char* data_{};  //NOLINT
    std::size_t capacity_{};  //NOLINT
    std::atomic_size_t size_{};  //NOLINT
    std::atomic_size_t committed_{};  //NOLINT
    std::atomic_size_t read_{};  //NOLINT
    std::function<void(std::string_view)> on_write_{};
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

    void set_on_write(std::function<void(std::string_view)> on_write);

    std::vector<std::shared_ptr<test_writer>> buffers_{};  //NOLINT
    std::size_t released_{};  //NOLINT
    std::function<void(std::string_view)> on_write_{};
};

class test_response : public response {
public:

    void code(response_code code) override;

    status body(std::string_view body) override;

    status body_head(std::string_view body_head) override;

    status acquire_channel(std::string_view name, data_channel*& ch) override;

    status release_channel(data_channel& ch) override;

    void set_on_write(std::function<void(std::string_view)> on_write);

    bool completed();

    [[nodiscard]] bool all_released() const noexcept;

    status close_session() override;;
    std::string body_{};  //NOLINT
    std::string body_head_{};  //NOLINT
    std::unique_ptr<test_channel> channel_{};  //NOLINT
    std::string message_{};  //NOLINT
    response_code code_{response_code::unknown};  //NOLINT
    std::atomic_bool completed_{};  //NOLINT
    std::size_t released_{};  //NOLINT
    std::function<void(std::string_view)> on_write_{}; //NOLINT
};

class test_endpoint : public provider {
public:

    status initialize(environment& env, void* context) {
        (void)env;
        (void)context;
        return tateyama::status::ok;
    }

    status shutdown() {
        return tateyama::status::ok;
    }
    static std::shared_ptr<provider> create() {
        return std::make_shared<test_endpoint>();
    }
};

}

register_component(endpoint, tateyama::api::endpoint::provider, mock, tateyama::api::endpoint::mock::test_endpoint::create);  //NOLINT
