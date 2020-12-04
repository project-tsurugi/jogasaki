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

#include <jogasaki/executor/global.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::data {

/**
 * @brief the store to hold result data from sql execution
 */
class result_store {
public:
    using store_type = data::iterable_record_store;
    using stores_type = std::vector<std::unique_ptr<store_type>>;
    using resources_type = std::vector<std::unique_ptr<memory::paged_memory_resource>>;
    /**
     * @brief create default object
     */
    result_store() = default;

    ~result_store() = default;
    result_store(result_store const& other) = delete;
    result_store& operator=(result_store const& other) = delete;
    result_store(result_store&& other) noexcept = default;
    result_store& operator=(result_store&& other) noexcept = default;

    /**
     * @brief accessor for size (number of partitions)
     */
    [[nodiscard]] store_type& store(std::size_t index) const noexcept {
        return *stores_[index];
    }

    /**
     * @brief extend the capacity so that the store holds data from multiple partitions
     * @param count the number of partitions
     * @param meta the metadata of the result record
     */
    void capacity(std::size_t count, maybe_shared_ptr<meta::record_meta> const& meta) {
        BOOST_ASSERT(stores_.empty());  //NOLINT
        meta_ = meta;
        stores_.reserve(count);
        result_record_resources_.reserve(count);
        result_varlen_resources_.reserve(count);
        for(std::size_t i=0; i < count; ++i) {
            auto& res = result_record_resources_.emplace_back(
                std::make_unique<memory::monotonic_paged_memory_resource>(&global::page_pool())
            );
            auto& varlen = result_varlen_resources_.emplace_back(
                std::make_unique<memory::monotonic_paged_memory_resource>(&global::page_pool())
            );
            stores_.emplace_back(std::make_unique<data::iterable_record_store>( res.get(), varlen.get(), meta));
        }
    }

    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept {
        return meta_;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return stores_.size();
    }
private:
    stores_type stores_{};
    resources_type result_record_resources_{};
    resources_type result_varlen_resources_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
};

}

