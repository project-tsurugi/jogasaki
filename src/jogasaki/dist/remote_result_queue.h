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

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <string>
#include <utility>

#include <jogasaki/utils/fail.h>

namespace jogasaki::dist {
/** @brief Bounded result messages between the RPC reader and scheduler worker. */
class remote_result_queue {
public:
    explicit remote_result_queue(std::size_t bytes = std::size_t{64} * 1024U * 1024U, std::size_t messages = 64)
        : byte_limit_(bytes), message_limit_(messages) {
        if (bytes == 0 || messages == 0) {
            fail_with_exception();
        }
    }
    bool push(std::string payload) {
        std::unique_lock lock{mutex_};
        if (payload.size() > byte_limit_) {
            return false;
        }
        space_.wait(lock, [&] {
            return cancelled_ || (messages_.size() < message_limit_ && payload.size() <= byte_limit_ - bytes_);
        });
        if (cancelled_) {
            return false;
        }
        bytes_ += payload.size();
        messages_.emplace_back(std::move(payload));
        return true;
    }
    std::deque<std::string> take() {
        std::scoped_lock lock{mutex_};
        std::deque<std::string> result{};
        result.swap(messages_);
        bytes_ = 0;
        space_.notify_all();
        return result;
    }
    bool empty() const {
        std::scoped_lock lock{mutex_};
        return messages_.empty();
    }
    void cancel() {
        // Predicate changes share the wait mutex, so cancellation cannot lose its wakeup.
        std::scoped_lock lock{mutex_};
        cancelled_ = true;
        space_.notify_all();
    }

private:
    std::size_t byte_limit_{};
    std::size_t message_limit_{};
    mutable std::mutex mutex_{};
    std::condition_variable space_{};
    std::deque<std::string> messages_{};
    std::size_t bytes_{};
    bool cancelled_{};
};
} // namespace jogasaki::dist
