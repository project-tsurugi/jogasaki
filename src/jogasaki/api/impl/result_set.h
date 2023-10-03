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

#include <jogasaki/executor/global.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::api::impl {

class result_set : public api::result_set {
public:
    explicit result_set(
        std::unique_ptr<data::result_store> store
    ) noexcept;

    [[nodiscard]] api::record_meta const* meta() const noexcept override;

    [[nodiscard]] std::unique_ptr<result_set_iterator> iterator() const override;

    void close() override;

private:
    std::unique_ptr<data::result_store> store_{};
    impl::record_meta meta_{};
};

}
