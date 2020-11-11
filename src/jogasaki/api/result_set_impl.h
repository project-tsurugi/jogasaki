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
#include <jogasaki/api/result_set.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::api {

class result_set::impl {
public:
    impl(
        std::shared_ptr<data::iterable_record_store> store,
        std::unique_ptr<memory::monotonic_paged_memory_resource> record_resource,
        std::unique_ptr<memory::monotonic_paged_memory_resource> varlen_resource
    ) noexcept :
        store_(std::move(store)),
        record_resource_(std::move(record_resource)),
        varlen_resource_(std::move(varlen_resource))
    {}

    explicit impl(
        std::shared_ptr<data::iterable_record_store> store
    ) noexcept :
        impl(
            std::move(store),
            std::make_unique<memory::monotonic_paged_memory_resource>(&global::page_pool()),
            std::make_unique<memory::monotonic_paged_memory_resource>(&global::page_pool())
        )
    {}

    [[nodiscard]] iterator begin();
    [[nodiscard]] iterator end();
    void close();

private:
    std::shared_ptr<data::iterable_record_store> store_{};
    std::unique_ptr<memory::monotonic_paged_memory_resource> record_resource_{};
    std::unique_ptr<memory::monotonic_paged_memory_resource> varlen_resource_{};
};

}
