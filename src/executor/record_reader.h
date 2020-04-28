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
#pragma once

#include <takatori/util/sequence_view.h>

#include <accessor/record_ref.h>

namespace dc::executor {

/**
 * @brief record reader interface for the process to retrieve record data
 * The data will be presented as record entry and next_record/get_record are used
 * to proceed the record position and to retrieve them.
 * At the beginning, initial position is set just before the first record entry (if any).
 */
class record_reader {
public:
    /**
     * @brief check whether next record entry is available
     * @return true when next data is available for reading
     * This guarantees following next_record() call won't return false.
     * @return false otherwise
     */
    [[nodiscard]] virtual bool available() const = 0;

    /**
     * @brief move the current position to the next record
     * @return true when next record entry exists and the position successfully moved forward
     * @return false when there is no next record
     * @pre either the following condition is met:
     * - no next_record() has been called since reader initialization
     * - most recent next_record() returned true
     * @warning the function behavior is undefined when pre-condition stated above is not met
     */
    virtual bool next_record() = 0;

    //FIXME: add get_status() to get error status if needed

    /**
     * @brief get the record at the current position
     * @return record reference to the record on the current position if next_record() was called beforehand and the results was true.
     * The returned record_ref will be invalidated when next_record() is called.
     * @pre next_record() has been called at least once and most recent call returned true
     * @warning the function behavior is undefined when pre-condition stated above is not met
     */
    [[nodiscard]] virtual accessor::record_ref get_record() const = 0;

    virtual void release() = 0;
    /**
     * @brief destructor
     */
    virtual ~record_reader() = default;
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(record_reader const& a, record_reader const& b) noexcept {
    return std::addressof(a) == std::addressof(b);
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(record_reader const& a, record_reader const& b) noexcept {
    return !(a == b);
}

}

