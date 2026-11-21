/*
 * Copyright 2026 Project Tsurugi.
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
#include <jogasaki/data/any.h>
#include <jogasaki/data/field_operations.h>
#include <jogasaki/meta/field_type.h>
namespace jogasaki::data {
/** Prepared search-key conversion. Preserve rounding and range information for SQL comparisons. */
class value_conversion {
public:
    [[nodiscard]] static value_conversion prepare(meta::field_type const& target);
    enum class ordering { exact, rounded_down, rounded_up, below_minimum, above_maximum, unsupported };
    struct result {
        ordering order{ordering::exact};
        any value{};
    };
    [[nodiscard]] result apply(any const& source) const;
private:
    using conversion_function = result (*)(any const&);
    value_conversion(conversion_function convert, field_operations operations)
        : convert_(convert), operations_(operations) {}
    conversion_function convert_{};
    field_operations operations_{};
};
}
