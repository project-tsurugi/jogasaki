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

#include <jogasaki/data/value.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/data/any.h>

namespace jogasaki::plan {

using jogasaki::data::any;

/**
 * @brief parameters for place holders
 */
class parameter_entry {
public:
    using kind = meta::field_type_kind;

    /**
     * @brief create new object
     */
    parameter_entry() = default;

    /**
     * @brief destruct the object
     */
    ~parameter_entry() = default;

    parameter_entry(parameter_entry const& other) = default;
    parameter_entry(parameter_entry&& other) noexcept = default;  //NOLINT(performance-noexcept-move-constructor,hicpp-noexcept-move)
    parameter_entry& operator=(parameter_entry const& other) = default;
    parameter_entry& operator=(parameter_entry&& other) noexcept = default;

    parameter_entry(
        meta::field_type type,
        data::value value
    );

    [[nodiscard]] meta::field_type const& type() const noexcept;
    [[nodiscard]] data::value const& value() const noexcept;
    [[nodiscard]] any as_any() const noexcept;

private:
    meta::field_type type_{};
    data::value value_{};
};

}
