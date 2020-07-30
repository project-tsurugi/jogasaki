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

#include <variant>
#include <boost/thread/thread.hpp>
#include <numa.h>

#include <takatori/util/universal_extractor.h>
#include <takatori/util/reference_list_view.h>

#include <jogasaki/model/step.h>
#include <jogasaki/model/port.h>

namespace jogasaki {

template<class T, class Variant, std::size_t index = 0>
[[nodiscard]] constexpr std::size_t alternative_index() noexcept {
    if constexpr (index == std::variant_size_v<Variant>) {  //NOLINT // clang tidy confused with if-constexpr
        return -1;
    } else if constexpr (std::is_same_v<std::variant_alternative_t<index, Variant>, T>) {  //NOLINT
        return index;
    } else {  //NOLINT
        return alternative_index<T, Variant, index + 1>();
    }
}

}

