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

#include <cstddef>
#include <utility>
#include <vector>

#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>

namespace jogasaki::data {

/**
 * @brief simple in-memory implementation of any_sequence_stream for mock table-valued functions.
 * @details this class provides a simple stream that returns pre-defined sequences from memory.
 *          it is primarily used for testing and mock functions.
 */
class mock_any_sequence_stream : public any_sequence_stream {
public:
    using sequences_type = std::vector<any_sequence>;

    /**
     * @brief constructs an empty stream.
     */
    mock_any_sequence_stream() = default;

    /**
     * @brief constructs a stream with pre-defined sequences.
     * @param sequences the sequences to return from the stream
     */
    explicit mock_any_sequence_stream(sequences_type sequences) noexcept;

    /**
     * @brief constructs a stream with initializer list of sequences.
     * @param sequences the sequences to return from the stream
     */
    mock_any_sequence_stream(std::initializer_list<any_sequence> sequences);

    [[nodiscard]] status_type try_next(any_sequence& sequence) override;

    [[nodiscard]] status_type next(
        any_sequence& sequence,
        std::optional<std::chrono::milliseconds> timeout) override;

    void close() override;

    /**
     * @brief resets the stream to the beginning.
     */
    void reset() noexcept;

    /**
     * @brief returns whether the stream is closed.
     * @return true if the stream is closed
     */
    [[nodiscard]] bool is_closed() const noexcept;

    /**
     * @brief returns the current position in the stream.
     * @return the current position
     */
    [[nodiscard]] std::size_t position() const noexcept;

    /**
     * @brief returns the total number of sequences in the stream.
     * @return the number of sequences
     */
    [[nodiscard]] std::size_t size() const noexcept;

private:
    sequences_type sequences_{};
    std::size_t position_{0};
    bool closed_{false};
};

}  // namespace jogasaki::data
