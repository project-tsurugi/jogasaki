/*
 * Copyright 2018-2026 Project Tsurugi.
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
#pragma once

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace tateyama::service_cli {

/**
 * @brief output shared by sessions
 * @details a block of lines is printed at once so that outputs from sessions don't interleave.
 * Each line is prefixed with optional timestamp and the session name.
 */
class output {
public:
    explicit output(bool timestamp) :
        timestamp_(timestamp)
    {}

    /**
     * @brief print lines as a block
     * @param session the session name used as prefix. Empty means no prefix.
     */
    void print(std::string_view session, std::vector<std::string> const& lines) {
        std::ostringstream ss{};
        auto ts = timestamp_ ? now() : std::string{};
        for(auto&& block : lines) {
            std::size_t pos = 0;
            // split multi-line text so that each line has prefix
            while(true) {
                auto next = block.find('\n', pos);
                auto line = std::string_view{block}.substr(pos, next == std::string::npos ? std::string::npos : next - pos);
                if (! ts.empty()) {
                    ss << ts << ' ';
                }
                if (! session.empty()) {
                    ss << session << ": ";
                }
                ss << line << '\n';
                if (next == std::string::npos) {
                    break;
                }
                pos = next + 1;
            }
        }
        std::lock_guard lk{mutex_};
        std::cout << ss.str() << std::flush;
    }

    void print(std::string_view session, std::string const& line) {
        print(session, std::vector<std::string>{line});
    }

private:
    bool timestamp_{};
    std::mutex mutex_{};

    static std::string now() {
        auto tp = std::chrono::system_clock::now();
        auto t = std::chrono::system_clock::to_time_t(tp);
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(tp.time_since_epoch()).count() % 1000000;
        std::tm tm{};
        localtime_r(&t, &tm);
        std::ostringstream ss{};
        ss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0') << std::setw(6) << us;
        return ss.str();
    }
};

}  // namespace tateyama::service_cli
