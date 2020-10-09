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

#include <cstdint>
#include <variant>
#include <jogasaki/accessor/text.h>

namespace jogasaki::executor::process::impl {

/**
 * @brief value store for any type
 */
class any {
public:
    any() = default;

    template<typename T, typename E = T>
    any(std::in_place_type_t<T>, E arg) : body_(std::in_place_type<T>, arg) {}

    template<typename T>
    T to() {
        return std::get<T>(body_);
    }
private:
    std::variant<
        bool,
        std::int32_t,
        std::int64_t,
        float,
        double,
        accessor::text
    > body_{};
};

}


