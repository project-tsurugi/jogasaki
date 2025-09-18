/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <decimal.hh>

#include <jogasaki/executor/less.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::mock {

/**
 * @brief customizable less functor for record fields
 * @details currently only the customization is comparing decimal field as triple
 */
struct custom_less {

    template <class T>
    std::enable_if_t<! std::is_same_v<T, runtime_t<meta::field_type_kind::decimal>>, bool> operator()(T const& x, T const& y) {
        return executor::less{}(x, y);
    }

    bool operator()(
        runtime_t<meta::field_type_kind::decimal> const& x,
        runtime_t<meta::field_type_kind::decimal> const& y
    ) {
        auto dx = static_cast<decimal::Decimal>(x);
        auto dy = static_cast<decimal::Decimal>(y);
        if (dx < dy) {
            return true;
        }
        if (dy < dx) {
            return false;
        }
        // x and y are same decimal value, but has different triple representation
        // let's define their order by the exponent
        return x.exponent() < y.exponent();
    }
};

}
