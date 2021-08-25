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

#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"
#include "schema.pb.h"

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
        size_ += length;
        return status::ok;
    }

    status commit() override {
        return status::ok;
    }

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

    explicit test_request(std::string_view payload) :
        payload_(payload)
    {}

    [[nodiscard]] std::string_view payload() const override {
        return payload_;
    }

    std::string payload_{};  //NOLINT
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
//        for(auto it=buffers_.begin(); it != buffers_.end(); ++it) {
//            if (it->get() == std::addressof(buf)) {
//                buffers_.erase(it);
//                break;
//            }
//        }
        return status::ok;
    }

    std::vector<std::shared_ptr<test_writer>> buffers_{};  //NOLINT
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
        body_.assign(body);
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

    std::string body_{};  //NOLINT
    std::unique_ptr<test_channel> channel_{};  //NOLINT
    std::string message_{};  //NOLINT
    response_code code_{};  //NOLINT
};

class payload {
public:
    void begin() {
        request_.set_allocated_begin(&begin_);
        session_.set_handle(0);
        request_.set_allocated_session_handle(&session_);
    }

    std::string_view build() {
        if (!request_.SerializeToString(&str_)) {
            std::abort();
        }
        request_.release_begin();
        request_.release_session_handle();
        return str_;
    }

    std::string str_{};
    ::request::Request request_{};
    ::request::Begin begin_{};
    ::common::Session session_{};

};
}
