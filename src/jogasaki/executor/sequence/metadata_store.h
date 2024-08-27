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

#include <cstddef>
#include <functional>
#include <memory>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/transaction.h>

namespace jogasaki::executor::sequence {

/**
 * @brief sequence metadata store
 */
class metadata_store {

public:
    using scan_consumer_type = std::function<void(std::size_t, std::size_t)>;

    /**
     * @brief create empty object
     */
    metadata_store();

    /**
     * @brief destruct object
     */
    ~metadata_store();

    metadata_store(metadata_store const& other) = delete;
    metadata_store& operator=(metadata_store const& other) = delete;
    metadata_store(metadata_store&& other) noexcept = delete;
    metadata_store& operator=(metadata_store&& other) noexcept = delete;

    /**
     * @brief create new object
     */
    explicit metadata_store(kvs::transaction& tx);

    /**
     * @brief put new entry for mapping from definition id to sequence id
     * @param def_id definition id of the sequence
     * @param id sequence id
     * @throws sequence::exception if any error occurs, then transaction held by this object is aborted.
     * This object should not be used any more.
     */
    void put(std::size_t def_id, std::size_t id);

    /**
     * @brief scan mapping
     * @param consumer consumer function for each mapping entry
     * @throws sequence::exception if any error occurs, then transaction held by this object is aborted.
     * This object should not be used any more.
     */
    void scan(scan_consumer_type const& consumer);

    /**
     * @brief find usable definition id
     * @param def_id [out] parameter filled with empty definition id available
     * @throws sequence::exception if any error occurs, then transaction held by this object is aborted.
     * This object should not be used any more.
     */
    void find_next_empty_def_id(std::size_t& def_id);

    /**
     * @brief remove the mappping entry for from definition id
     * @param def_id definition id of the sequence to be removed
     * @return true if successful
     * @return false if entry is not found
     * @throws sequence::exception if any error occurs, then transaction held by this object is aborted.
     * This object should not be used any more.
     */
    bool remove(std::size_t def_id);

    /**
     * @brief get the size of the mapping
     * @return the number of mapping entries
     */
    [[nodiscard]] std::size_t size();

private:
    std::unique_ptr<kvs::storage> stg_{};
    kvs::transaction* tx_{};
};

}  // namespace jogasaki::executor::sequence
