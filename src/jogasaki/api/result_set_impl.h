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
#include <jogasaki/data/result_store.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::api {

class result_set::impl {
public:
    explicit impl(
        std::unique_ptr<data::result_store> store
    ) noexcept :
        store_(std::move(store))
    {}

    [[nodiscard]] maybe_shared_ptr<meta::record_meta> meta() const noexcept;

    [[nodiscard]] iterator begin();
    [[nodiscard]] iterator end();
    void close();

private:
    std::unique_ptr<data::result_store> store_{};
};

}
