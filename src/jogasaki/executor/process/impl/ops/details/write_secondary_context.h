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

#include <vector>
#include <memory>

#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/storage.h>

namespace jogasaki::executor::process::impl::ops::details {

/**
 * @brief partial write operator context
 */
class write_secondary_context {
public:
    friend class write_secondary_target;

    using memory_resource = memory::lifo_paged_memory_resource;
    /**
     * @brief create empty object
     */
    write_secondary_context() = default;

    /**
     * @brief create new object
     */
    explicit write_secondary_context(
        std::unique_ptr<kvs::storage> stg
    ) :
        stg_(std::move(stg))
    {}

private:
    std::unique_ptr<kvs::storage> stg_{};
    data::aligned_buffer key_buf_{};
};

}
