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

#include <cstddef>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/executor/hash.h>

namespace jogasaki::executor {

using takatori::util::maybe_shared_ptr;

/**
 * @brief partitioner determines input partition for the record to be sent
 */
class partitioner {
public:
    /**
     * @brief construct empty object
     */
    partitioner() = default;

    /**
     * @brief construct new object
     * @param partitions number of total partitions
     * @param meta schema information of the record whose target is calculated by this partitioner
     */
    partitioner(std::size_t partitions, maybe_shared_ptr<meta::record_meta> meta) noexcept;

    /**
     * @brief retrieve the partition for the given record
     * @param key the record to be evaluated
     * @return the target partition number
     */
    [[nodiscard]] std::size_t operator()(accessor::record_ref key) const noexcept;

private:
    std::size_t partitions_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
    hash hash_{};

    using kind = meta::field_type_kind;

    [[nodiscard]] std::size_t field_hash(accessor::record_ref key, std::size_t field_index) const;
};

}
