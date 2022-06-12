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
#include "value.h"

namespace jogasaki::data {

value::operator bool() const noexcept {
    return !empty();
}

bool value::empty() const noexcept {
    return body_.index() == 0;
}

std::size_t value::type_index() const noexcept {
    return body_.index();
}

any value::view() const {
    switch(type_index()) {
        case index<std::monostate>: return any{};
        case index<std::int8_t>: return any{std::in_place_type<std::int8_t>, ref<std::int8_t>()};
        case index<std::int16_t>: return any{std::in_place_type<std::int16_t>, ref<std::int16_t>()};
        case index<std::int32_t>: return any{std::in_place_type<std::int32_t>, ref<std::int32_t>()};
        case index<std::int64_t>: return any{std::in_place_type<std::int64_t>, ref<std::int64_t>()};
        case index<float>: return any{std::in_place_type<float>, ref<float>()};
        case index<double>: return any{std::in_place_type<double>, ref<double>()};
        case index<std::string>: return any{std::in_place_type<accessor::text>, accessor::text{ref<std::string>()}};
        case index<std::size_t>: return any{std::in_place_type<std::size_t>, ref<std::size_t>()};
        default: fail();
    }
    fail();
}


}


