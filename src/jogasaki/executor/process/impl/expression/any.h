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
#include <ios>

#include <glog/logging.h>
#include <takatori/util/fail.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/process/impl/expression/error.h>

namespace jogasaki::executor::process::impl::expression {

using takatori::util::fail;

/**
 * @brief value store for any type
 */
class any {
public:
    /**
     * @brief construct empty instance
     */
    any() = default;

    /**
     * @brief construct new instance
     */
    template<typename T, typename E = T>
    any(std::in_place_type_t<T>, E arg) : body_(std::in_place_type<T>, arg) {}

    /**
     * @brief accessor of the content value in given type
     */
    template<typename T>
    T to() {
        if(auto* p = std::get_if<T>(&body_); p != nullptr) {
            return *p;
        }
        fail();
    }

    /**
     * @brief return whether non-error value is assigned to this object
     */
    [[nodiscard]] explicit operator bool() const noexcept {
        return has_value() && !error();
    }

    /**
     * @brief return whether any value is assigned to this object
     */
    [[nodiscard]] bool has_value() const noexcept {
        return body_.index() != 0;
    }

    /**
     * @brief return whether any value is assigned to this object
     */
    [[nodiscard]] bool error() const noexcept {
        return body_.index() == 1;
    }

private:
    std::variant<
        std::monostate,
        class error,
        bool,
        std::int32_t,
        std::int64_t,
        float,
        double,
        accessor::text
    > body_{};
};

static_assert(!std::is_trivially_copyable_v<any>); //P0602R4 is not available until gcc 8.3
static_assert(std::is_trivially_destructible_v<any>);
static_assert(std::alignment_of_v<any> == 8);
static_assert(sizeof(any) == 24);

template<>
inline any::any(std::in_place_type_t<bool>, std::int8_t arg) : body_(std::in_place_type<bool>, arg != 0) {}

}
