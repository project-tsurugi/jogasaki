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
#include "mock_any_sequence_stream.h"

#include <utility>

namespace jogasaki::data {

mock_any_sequence_stream::mock_any_sequence_stream(sequences_type sequences) noexcept :
    sequences_(std::move(sequences))
{}

mock_any_sequence_stream::mock_any_sequence_stream(
    std::initializer_list<any_sequence> sequences) :
    sequences_(sequences)
{}

any_sequence_stream::status_type mock_any_sequence_stream::try_next(any_sequence& sequence) {
    if (closed_) {
        return status_type::end_of_stream;
    }
    if (position_ >= sequences_.size()) {
        return status_type::end_of_stream;
    }
    sequence = sequences_[position_];
    ++position_;
    return status_type::ok;
}

any_sequence_stream::status_type mock_any_sequence_stream::next(
    any_sequence& sequence,
    std::optional<std::chrono::milliseconds> /* timeout */) {
    // for in-memory stream, next() behaves the same as try_next()
    // since data is always available immediately
    return try_next(sequence);
}

void mock_any_sequence_stream::close() {
    closed_ = true;
}

void mock_any_sequence_stream::reset() noexcept {
    position_ = 0;
    closed_ = false;
}

bool mock_any_sequence_stream::is_closed() const noexcept {
    return closed_;
}

std::size_t mock_any_sequence_stream::position() const noexcept {
    return position_;
}

std::size_t mock_any_sequence_stream::size() const noexcept {
    return sequences_.size();
}

}  // namespace jogasaki::data
