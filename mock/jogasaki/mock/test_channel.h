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

#include <regex>
#include <thread>

#include <takatori/util/downcast.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>

namespace jogasaki::api {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class test_writer : public api::writer {

public:
    test_writer() = default;

    explicit test_writer(std::size_t write_latency_ms) :
        write_latency_ms_(write_latency_ms)
    {}

    status write(char const* data, std::size_t length) override {
        BOOST_ASSERT(size_+length <= data_.max_size());  //NOLINT
        std::memcpy(data_.data()+size_, data, length);
        size_ += length;
        if (write_latency_ms_ > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds{write_latency_ms_});
        }
        return status::ok;
    }

    status commit() override {
        return status::ok;
    }

    std::array<char, 4096> data_{};  //NOLINT
    std::size_t capacity_{};  //NOLINT
    std::size_t size_{};  //NOLINT
    std::size_t write_latency_ms_{};
};

class test_channel : public api::data_channel {
public:
    test_channel() = default;

    explicit test_channel(std::size_t write_latency_ms) :
        write_latency_ms_(write_latency_ms)
    {}

    status acquire(writer*& buf) override {
        auto& s = writers_.emplace_back(std::make_shared<test_writer>(write_latency_ms_));
        buf = s.get();
        return status::ok;
    }

    status release(writer& buf) override {
        all_writers_released_ = true;
        return status::ok;
    }

    bool all_writers_released() const noexcept {
        return all_writers_released_;
    }

    std::vector<std::shared_ptr<test_writer>> writers_{};  //NOLINT
    bool all_writers_released_{};
    std::size_t write_latency_ms_{};
};

}
