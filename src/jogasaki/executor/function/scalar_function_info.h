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

#include <cstddef>
#include <functional>
#include <memory>
#include <set>
#include <vector>

#include <takatori/util/sequence_view.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/value_store.h>
#include <jogasaki/executor/function/scalar_function_kind.h>
#include <jogasaki/executor/function/value_generator.h>

namespace jogasaki::executor::function {

using takatori::util::sequence_view;

/**
 * @brief definition of scalar function type
 */
using scalar_function_type = std::function<data::any (sequence_view<data::any>)>;

/**
 * @brief scalar function information interface
 */
class scalar_function_info {
public:
    scalar_function_info() = default;
    ~scalar_function_info() = default;
    scalar_function_info(scalar_function_info const& other) = default;
    scalar_function_info& operator=(scalar_function_info const& other) = default;
    scalar_function_info(scalar_function_info&& other) noexcept = default;
    scalar_function_info& operator=(scalar_function_info&& other) noexcept = default;

    /**
     * @brief create new object
     * @param kind kind of the scalar function
     */
    scalar_function_info(
        scalar_function_kind kind,
        scalar_function_type function_body,
        std::size_t arg_count = 1
    );

    /**
     * @brief accessor to scalar function kind
     * @return the kind of the scalar function
     */
    [[nodiscard]] constexpr scalar_function_kind kind() const noexcept {
        return kind_;
    }

    /**
     * @brief accessor to scalar function
     */
    [[nodiscard]] scalar_function_type const& function_body() const noexcept;

    /**
     * @brief accessor to arg count
     */
    [[nodiscard]] std::size_t arg_count() const noexcept;

private:
    scalar_function_kind kind_{};
    scalar_function_type function_body_{};
    std::size_t arg_count_{};
};

}  // namespace jogasaki::executor::function
