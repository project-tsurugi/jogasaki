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

#include <ostream>
#include <string_view>

namespace jogasaki::data {

/**
 * @brief represents the status of any_sequence retrieval from any_sequence_stream.
 */
enum class any_sequence_stream_status {

    /**
     * @brief a sequence was successfully retrieved.
     */
    ok,

    /**
     * @brief an error occurred during retrieval.
     */
    error,

    /**
     * @brief the end of the stream has been reached.
     */
    end_of_stream,

    /**
     * @brief the next sequence is not yet available.
     */
    not_ready,
};

/**
 * @brief returns the string representation of the given status.
 * @param value the target value
 * @return the string representation
 */
[[nodiscard]] constexpr std::string_view to_string_view(any_sequence_stream_status value) noexcept {
    using kind = any_sequence_stream_status;
    switch (value) {
        case kind::ok: return "ok";
        case kind::error: return "error";
        case kind::end_of_stream: return "end_of_stream";
        case kind::not_ready: return "not_ready";
    }
    return "unknown";
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output stream
 */
inline std::ostream& operator<<(std::ostream& out, any_sequence_stream_status value) {
    return out << to_string_view(value);
}

}  // namespace jogasaki::data
