/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <emmintrin.h>
#include <functional>
#include <ios>
#include <memory>
#include <ratio>
#include <regex>
#include <string>
#include <string_view>
#include <vector>
#include <xmmintrin.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>

#include <tateyama/api/server/data_channel.h>
#include <tateyama/api/server/database_info.h>
#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>
#include <tateyama/api/server/session_info.h>
#include <tateyama/api/server/session_store.h>
#include <tateyama/api/server/writer.h>
#include <tateyama/proto/diagnostics.pb.h>
#include <tateyama/status.h>

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

class database_info_impl : public tateyama::api::server::database_info {
public:
    process_id_type process_id() const noexcept override {
        return {};
    }

    std::string_view name() const noexcept override {
        return "database-name";
    }

    time_type start_at() const noexcept override {
        return {};
    }
};

class session_info_impl : public tateyama::api::server::session_info {
public:
    id_type id() const noexcept override {
        return {};
    }

    std::string_view label() const noexcept override {
        return "label";
    }

    std::string_view application_name() const noexcept override {
        return "application-name";
    }

    std::string_view user_name() const noexcept override {
        return "user-name";
    }

    time_type start_at() const noexcept override {
        return {};
    }

    std::string_view connection_type_name() const noexcept override {
        return "connection-type-name";
    }

    std::string_view connection_information() const noexcept override {
        return "connection-information";
    }
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
    [[nodiscard]] std::size_t service_id() const override;
    [[nodiscard]] std::size_t local_id() const override;
    [[nodiscard]] std::string_view payload() const override;
    [[nodiscard]] tateyama::api::server::database_info const& database_info() const noexcept override;
    [[nodiscard]] tateyama::api::server::session_info const& session_info() const noexcept override;
    [[nodiscard]] tateyama::api::server::session_store& session_store() noexcept override;
    [[nodiscard]] tateyama::session::session_variable_set& session_variable_set() noexcept override;

    [[nodiscard]] bool has_blob(std::string_view channel_name) const noexcept override;
    [[nodiscard]] blob_info const& get_blob(std::string_view name) const override;

    std::string payload_{};  //NOLINT
    std::size_t session_id_{};
    std::size_t service_id_{};
    database_info_impl database_info_{};
    session_info_impl session_info_{};
    tateyama::api::server::session_store session_store_{};
    tateyama::session::session_variable_set session_variable_set_{};

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

    status body(std::string_view body) override;

    status body_head(std::string_view body_head) override;

    void error(proto::diagnostics::Record const& record) override;

    status acquire_channel(std::string_view name, std::shared_ptr<data_channel>& ch,
        std::size_t max_writer_count) override;

    status release_channel(data_channel& ch) override;

    void set_on_write(std::function<void(std::string_view)> on_write);

    bool completed();

    void session_id(std::size_t id) override;

    template <class Rep = std::int64_t, class Period = std::milli>
    bool wait_completion(std::chrono::duration<Rep, Period> dur = std::chrono::milliseconds{2000}) {
        using clock = std::chrono::steady_clock;
        auto begin = clock::now();
        auto cnt = 0;
        while(! completed_) {
            ++cnt;
            if(cnt % 10000 == 0) {
                auto end = clock::now();
                if(end-begin > dur) {
                    return false;
                }
            }
            _mm_pause();
        }
        return true;
    }

    [[nodiscard]] bool all_released() const noexcept;

    [[nodiscard]] bool check_cancel() const override;

    void cancel();

    [[nodiscard]] status add_blob(std::unique_ptr<blob_info> blob) override;

    std::string body_{};  //NOLINT
    std::string body_head_{};  //NOLINT
    std::shared_ptr<test_channel> channel_{};  //NOLINT
    std::string message_{};  //NOLINT
    std::atomic_bool completed_{};  //NOLINT
    std::size_t released_{};  //NOLINT
    std::function<void(std::string_view)> on_write_{}; //NOLINT
    std::size_t session_id_{};  //NOLINT
    proto::diagnostics::Record error_{};  //NOLINT
    std::atomic_bool cancel_requested_{};  //NOLINT
};

}  // namespace tateyama::api::server::mock
