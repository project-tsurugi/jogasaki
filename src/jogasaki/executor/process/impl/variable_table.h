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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>

namespace jogasaki::executor::process::impl {

using takatori::util::maybe_shared_ptr;

/**
 * @brief variables storage
 */
class variable_table {
public:
    /**
     * @brief construct empty instance
     */
    variable_table() = default;

    /**
     * @brief construct new instance
     */
    explicit variable_table(
        variable_table_info const& info
    );

    /**
     * @brief accessor to variable store
     */
    [[nodiscard]] data::small_record_store& store() const noexcept;

    /**
     * @brief accessor to metadata of variable store
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept;

    /**
     * @brief accessor to variable table info
     */
    [[nodiscard]] variable_table_info const& info() const noexcept;

    /**
     * @brief return whether the object is non-empty
     */
    [[nodiscard]] explicit operator bool() const noexcept;

private:
    variable_table_info const* info_{};
    std::unique_ptr<data::small_record_store> store_{};
};

}


