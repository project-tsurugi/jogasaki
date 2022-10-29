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
#include <tateyama/api/server/writer.h>
#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>
#include <tateyama/api/server/data_channel.h>
#include <tateyama/api/server/response_code.h>

#include <memory>
#include <regex>
#include <atomic>

#include <jogasaki/utils/latch.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>

namespace tateyama::api::server::mock {

using namespace std::literals::string_literals;
using namespace std::string_view_literals;

std::string_view view_of(std::stringstream& stream);

inline void reset_ss(std::stringstream& ss) {
    ss.str("");
    ss.clear();
}

class buffer_manager {
    tbb::concurrent_hash_map<std::stringstream*, std::shared_ptr<std::stringstream>> entity_{};
    tbb::concurrent_queue<std::stringstream*> queue_{};

public:
    buffer_manager() = default;

    bool acquire(std::stringstream*& bufp);

    bool release(std::stringstream* bufp);
};

class test_writer : public writer {
public:
    test_writer() = default;

    ~test_writer();

    status write(char const* data, std::size_t length) override;

    status commit() override;

    std::string_view read();

    void set_on_write(std::function<void(std::string_view)> on_write);

    [[nodiscard]] std::string_view view() const noexcept;
private:
    std::stringstream* buf_{};
    std::function<void(std::string_view)> on_write_{};
    std::atomic_size_t size_{};  //NOLINT
    std::atomic_size_t committed_{};  //NOLINT
    std::atomic_size_t read_{};  //NOLINT
};

class test_request : public request {
public:
    test_request() = default;

    explicit test_request(std::string_view payload);
    test_request(
        std::string_view payload,
        std::size_t session_id,
        std::size_t service_id
    ) :
        payload_(payload),
        session_id_(session_id),
        service_id_(service_id)
    {}


    [[nodiscard]] std::size_t session_id() const override;
    [[nodiscard]] std::size_t service_id() const;
    [[nodiscard]] std::string_view payload() const override;

    std::string payload_{};  //NOLINT
    std::size_t session_id_{};
    std::size_t service_id_{};
};

class test_channel : public data_channel {
public:
    test_channel() = default;

    status acquire(std::shared_ptr<writer>& wrt) override;

    status release(writer& buf) override;

    [[nodiscard]] bool all_released() const noexcept;

    void set_on_write(std::function<void(std::string_view)> on_write);

    std::vector<std::string_view> view() const;

    std::vector<std::shared_ptr<test_writer>> buffers_{};  //NOLINT
    std::size_t released_{};  //NOLINT
    std::function<void(std::string_view)> on_write_{};
};

class test_response : public response {
public:

    void code(response_code code) override;

    status body(std::string_view body) override;

    status body_head(std::string_view body_head) override;

    status acquire_channel(std::string_view name, std::shared_ptr<data_channel>& ch) override;

    status release_channel(data_channel& ch) override;

    void set_on_write(std::function<void(std::string_view)> on_write);

    bool completed();

    void session_id(std::size_t id);

    template <class Rep = std::int64_t, class Period = std::milli>
    bool wait_completion(std::chrono::duration<Rep, Period> dur = std::chrono::milliseconds{2000}) {
        return completion_latch_.wait(dur);
    }

    [[nodiscard]] bool all_released() const noexcept;

    status close_session() override;;
    std::string body_{};  //NOLINT
    std::string body_head_{};  //NOLINT
    std::shared_ptr<test_channel> channel_{};  //NOLINT
    std::string message_{};  //NOLINT
    response_code code_{response_code::unknown};  //NOLINT
    std::atomic_bool completed_{};  //NOLINT
    jogasaki::utils::latch completion_latch_{};  //NOLINT
    std::size_t released_{};  //NOLINT
    std::function<void(std::string_view)> on_write_{}; //NOLINT
    std::size_t session_id_{};  //NOLINT
};

}

