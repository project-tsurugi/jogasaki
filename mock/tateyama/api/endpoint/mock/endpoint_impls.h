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

    test_writer(char* data, std::size_t capacity) :
        data_(data), capacity_(capacity)
    {}

    status write(char const* data, std::size_t length) override {
        std::memcpy(data_ + size_, data, length);
        return status::ok;
    }

    status commit() override {
        return status::ok;
    }

protected:
    char* data_{};
    std::size_t capacity_{};
    std::size_t size_{};
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

    explicit test_request(std::string_view payload) :
        payload_(payload)
    {}

    std::string_view payload() override {
        return payload_;
    }

    std::string payload_{};
};

class test_channel : public data_channel {
public:
    test_channel() = default;

    status acquire(writer*& buf) override {
        auto& s = buffers_.emplace_back(std::make_shared<fixed_buffer_writer<100>>());
        buf = s.get();
        return status::ok;
    }

    status release(writer& buf) override {
        for(auto it=buffers_.begin(); it != buffers_.end(); ++it) {
            if (it->get() == std::addressof(buf)) {
                buffers_.erase(it);
                break;
            }
        }
        return status::ok;
    }

private:
    std::vector<std::shared_ptr<test_writer>> buffers_{};
};

class test_response : public response {
public:

    void code(response_code code) override {
        code_ = code;
    }

    void message(std::string_view msg) override {
        message_ = msg;
    }

    status complete() override {
        return status::ok;
    }

    status body(std::string_view body) override {
        body_.clear();
        body_.write(body.data(), body.size());
        return status::ok;
    }

    status acquire_channel(std::string_view name, data_channel*& ch) override {
        (void) name;
        channel_ = std::make_unique<test_channel>();
        ch = channel_.get();
        return status::ok;
    }

    status release_channel(data_channel& ch) override {
        (void)ch;
        return status::ok;
    }

private:
    std::stringstream body_{};
    std::unique_ptr<data_channel> channel_{};
    std::string message_{};
    response_code code_{};
};

}
