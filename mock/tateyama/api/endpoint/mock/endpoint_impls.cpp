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
#include <tateyama/api/endpoint/mock/endpoint_impls.h>

#include <memory>
#include <regex>

namespace tateyama::api::endpoint::mock {

using namespace std::literals::string_literals;
using namespace std::string_view_literals;

test_writer::test_writer(char* data, std::size_t capacity) :
    data_(data), capacity_(capacity)
{}

status test_writer::write(char const* data, std::size_t length) {
    std::memcpy(data_ + size_, data, length);
    if (on_write_) {
        on_write_(std::string_view{data_ + size_, length});
    }
    size_ += length;
    return status::ok;
}

status test_writer::commit() {
    committed_.store(size_.load());
    return status::ok;
}

std::string_view test_writer::read() {
    std::size_t sz = committed_ - read_;
    if (sz > 0) {
        read_.store(committed_.load());
        return {data_+read_, sz};
    }
    return {data_+read_, 0};
}

void test_writer::set_on_write(std::function<void(std::string_view)> on_write) {
    on_write_ = std::move(on_write);
}

test_request::test_request(std::string_view payload) :
    payload_(payload)
{}

std::string_view test_request::payload() const {
    return payload_;
}

status test_channel::acquire(writer*& buf) {
    auto& s = buffers_.emplace_back(std::make_shared<fixed_buffer_writer<1024*1024>>());
    if (on_write_) {
        s->set_on_write(on_write_);
    }
    buf = s.get();
    return status::ok;
}

status test_channel::release(writer&) {
    ++released_;
    return status::ok;
}

void test_response::code(response_code code) {
    code_ = code;
}

status test_response::body(std::string_view body) {
    body_.assign(body);
    completed_ = true;
    return status::ok;
}

status test_response::body_head(std::string_view body_head) {
    body_head_.assign(body_head);
    return status::ok;
}

status test_response::acquire_channel(std::string_view name, data_channel*& ch) {
    (void) name;
    channel_ = std::make_unique<test_channel>();
    if (on_write_) {
        channel_->set_on_write(on_write_);
    }
    ch = channel_.get();
    return status::ok;
}

status test_response::release_channel(data_channel&) {
    ++released_;
    return status::ok;
}

bool test_response::completed() {
    return completed_.load();
}

status test_response::close_session() {
    return status::ok;
}

bool test_response::all_released() const noexcept {
    return !channel_ || released_ > 0;
}

void test_response::set_on_write(std::function<void(std::string_view)> on_write) {
    on_write_ = std::move(on_write);
}

bool test_channel::all_released() const noexcept {
    return buffers_.size() == released_;
}

void test_channel::set_on_write(std::function<void(std::string_view)> on_write) {
    on_write_ = std::move(on_write);
}
}
