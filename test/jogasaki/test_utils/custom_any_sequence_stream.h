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

#include <functional>
#include <optional>
#include <chrono>

#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>

namespace jogasaki::testing {

/**
 * @brief an any_sequence_stream whose try_next behaviour is fully controlled by a caller-supplied lambda.
 * @details the lambda is invoked on every try_next call and returns the desired status, optionally
 *          filling the sequence.  this makes it easy to inject not_ready transitions, cancel signals,
 *          or error conditions at precisely the desired point in a test scenario.
 *
 * example — return not_ready on the first call, then end_of_stream:
 * @code
 *   int calls = 0;
 *   auto stream = std::make_unique<custom_any_sequence_stream>(
 *       [calls](data::any_sequence&) mutable -> data::any_sequence_stream::status_type {
 *           if (calls++ == 0) {
 *               return data::any_sequence_stream::status_type::not_ready;
 *           }
 *           return data::any_sequence_stream::status_type::end_of_stream;
 *       }
 *   );
 * @endcode
 */
class custom_any_sequence_stream : public data::any_sequence_stream {
public:
    using handler_type = std::function<status_type(data::any_sequence&)>;

    /**
     * @brief constructs the stream with a custom try_next handler.
     * @param handler called on every try_next invocation.
     */
    explicit custom_any_sequence_stream(handler_type handler) :
        handler_(std::move(handler))
    {}

    [[nodiscard]] status_type try_next(data::any_sequence& sequence) override {
        return handler_(sequence);
    }

    [[nodiscard]] status_type next(
        data::any_sequence& sequence,
        std::optional<std::chrono::milliseconds> /* timeout */) override {
        return try_next(sequence);
    }

    void close() override {}

private:
    handler_type handler_{};
};

}  // namespace jogasaki::testing
