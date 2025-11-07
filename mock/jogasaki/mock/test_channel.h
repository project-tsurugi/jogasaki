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

#include <regex>
#include <thread>
#include <mutex>

#include <takatori/util/downcast.h>

#include <jogasaki/logging.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/utils/binary_printer.h>

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
        VLOG(log_debug) << "write " << utils::binary_printer{data_.data()+size_, length};
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
    std::size_t write_latency_ms_{};  //NOLINT
};

class test_channel : public api::data_channel {
public:
    test_channel() = default;

    explicit test_channel(std::size_t write_latency_ms) :
        write_latency_ms_(write_latency_ms)
    {}

    status acquire(std::shared_ptr<writer>& w) override {
        std::unique_lock lk{mutex_};
        w = writers_.emplace_back(std::make_shared<test_writer>(write_latency_ms_));
        return status::ok;
    }

    status release(writer&) override {
        std::unique_lock lk{mutex_};
        ++released_;
        return status::ok;
    }

    [[nodiscard]] bool all_writers_released() const noexcept {
        std::unique_lock lk{mutex_};
        return writers_.size() == released_;
    }

    [[nodiscard]] std::optional<std::size_t> max_writer_count() override {
        return max_writer_count_;
    }

    mutable std::mutex mutex_{};
    std::vector<std::shared_ptr<test_writer>> writers_{};  //NOLINT
    std::size_t released_{};  //NOLINT
    std::size_t write_latency_ms_{};  //NOLINT
    std::optional<std::size_t> max_writer_count_{};
};

}
