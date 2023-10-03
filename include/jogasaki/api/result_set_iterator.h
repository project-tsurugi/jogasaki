/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <jogasaki/api/record.h>

namespace jogasaki::api {

/**
 * @brief iterator over result records
 * @details At the beginning, the iterator is on top of the result set pointing no record,
 * and next() will move the position to the first record.
 */
class result_set_iterator {
public:
    /**
     * @brief construct
     */
    result_set_iterator() = default;

    /**
     * @brief copy construct
     */
    result_set_iterator(result_set_iterator const&) = delete;

    /**
     * @brief move construct
     */
    result_set_iterator(result_set_iterator &&) = default;

    /**
     * @brief copy assign
     */
    result_set_iterator& operator=(result_set_iterator const&) = delete;

    /**
     * @brief move assign
     */
    result_set_iterator& operator=(result_set_iterator &&) = default;

    /**
     * @brief destruct result_set_iterator
     */
    virtual ~result_set_iterator() = default;

    /**
     * @brief provides whether the next record exists
     * @return whether the next record exists
     */
    [[nodiscard]] virtual bool has_next() const = 0;

    /**
     * @brief move the iterator to the next record and return accessor to it.
     * @return the record at the current (i.e. newly moved to) position. The returned record pointer becomes
     * invalid when the iterator is moved by another next() call, or when result_set is closed.
     * @return nullptr if no more record exists
     */
    [[nodiscard]] virtual record* next() = 0;

};

}
