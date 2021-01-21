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

#include <jogasaki/api/field_type.h>
#include <jogasaki/meta/field_type.h>

namespace jogasaki::api::impl {

/**
 * @brief type information for a field
 */
class field_type : public api::field_type {
public:

    /**
     * @brief construct empty object (kind undefined)
     */
    constexpr field_type() noexcept = default;

    /**
     * @brief construct empty object (kind undefined)
     */
    explicit field_type(meta::field_type type) noexcept;

    /**
     * @brief getter for type kind
     */
    [[nodiscard]] api::field_type_kind kind() const noexcept override;;

private:
    meta::field_type type_{};
};

} // namespace

