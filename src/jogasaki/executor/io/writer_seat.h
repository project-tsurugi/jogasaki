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
#pragma once

#include <memory>

#include <jogasaki/executor/io/record_writer.h>

namespace jogasaki::executor::io {

class record_channel;

/**
 * @brief represents a single seat that can hold a record_writer instance
 * @details A reserved seat models the right to use (acquire) a writer. 
 * Non-reserved seats are not associated with writers and they are just placeholders for writer_pool::acquire().
 * The internal writer instance is created lazily when first needed to avoid unnecessary allocations.
 * @note this object is not thread-safe and only one thread should access it at a time.
 */
class writer_seat {
public:
    /**
     * @brief construct a new seat
     * @param channel optional channel used to materialize writers (may be nullptr)
     * @param reserved whether the seat is reserved
     */
    explicit constexpr writer_seat(record_channel* channel = nullptr, bool reserved = false) noexcept :
        channel_(channel),
        reserved_(reserved)
    {}

    /**
     * @brief destroy the object
     */
    ~writer_seat() = default;

    writer_seat(writer_seat const& other) = delete;
    writer_seat& operator=(writer_seat const& other) = delete;

    /**
     * @brief move construct
     * @param other moved target, which will be empty (i.e. default initialized) after the move
     */
    writer_seat(writer_seat&& other) noexcept;

    /**
     * @brief move assign
     * @param other moved target, which will be empty (i.e. default initialized) after the move
     * @return self
     */
    writer_seat& operator=(writer_seat&& other) noexcept;

    /**
     * @brief check whether the seat is reserved (i.e. holding the right to use/acquire a writer)
     * @return true if the seat is reserved
     * @return false otherwise
     */
    [[nodiscard]] bool reserved() const noexcept;

    /**
     * @brief acquire the new writer if necessary and return its reference 
     * @details after calling this function, the seat will hold the writer instance. The writer is
     * lazily acquired from the underlying channel the first time this function is called.
     * @return reference to the acquired writer
     * @pre the seat must be reserved. Otherwise, behavior is undefined.
     * @post after calling this function, has_writer() returns true.
     */
    std::shared_ptr<record_writer> const& writer();

    /**
     * @brief check whether the seat is occupied with the writer acquired
     * @details this function returns true if the seat already holds the writer instance, i.e. writer() has been called before.
     * @return true if the seat holds a writer
     * @return false otherwise
     */
    [[nodiscard]] bool has_writer() const noexcept;

private:
    record_channel* channel_{};
    std::shared_ptr<record_writer> writer_{};
    bool reserved_{};
};

}  // namespace jogasaki::executor::io
