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

#include <memory>
#include <vector>

#include <jogasaki/api/record_meta.h>
#include <jogasaki/api/result_set_iterator.h>

namespace jogasaki::api {

/**
 * @brief result set interface to provide iterator/disposal method for the query result
 */
class result_set {
public:
    /**
     * @brief create new object
     */
    result_set() = default;

    /**
     * @brief destruct object
     */
    virtual ~result_set() = default;

    result_set(result_set const& other) = delete;
    result_set& operator=(result_set const& other) = delete;
    result_set(result_set&& other) noexcept = delete;
    result_set& operator=(result_set&& other) noexcept = delete;

    /**
     * @brief accessor to the metadata of the result records
     * @return the record metadata
     */
    [[nodiscard]] virtual api::record_meta const* meta() const noexcept = 0;

    /**
     * @brief getter of the iterator at the beginning of the result records
     * @return the iterator object on the result records
     */
    [[nodiscard]] virtual std::unique_ptr<result_set_iterator> iterator() const = 0;

    /**
     * @brief close and release the resources for this result set
     * @details This method must be called when the query result is not needed any more in order to avoid resource leak.
     * @note iterators and metadata for this result set becomes invalid and accessing after this call causes undefined behavior
     */
    virtual void close() = 0;
};

}
