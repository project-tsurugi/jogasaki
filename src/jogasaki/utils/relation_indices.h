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

#include <takatori/plan/step.h>
#include <takatori/plan/graph.h>
#include <takatori/descriptor/relation.h>
#include <takatori/util/fail.h>
#include <yugawara/binding/extract.h>

namespace jogasaki::utils {

using takatori::util::fail;

constexpr static std::size_t npos = static_cast<std::size_t>(-1);

[[nodiscard]] inline std::size_t find_input_index(
    takatori::plan::step const& self,
    takatori::descriptor::relation const& target
) {
    std::size_t ret = npos;
    if (auto t = yugawara::binding::extract_if<::takatori::plan::exchange>(target)) {
        std::size_t count = 0;
        takatori::plan::enumerate_upstream(self, [&count, &t, &ret](takatori::plan::step const& e){
            if (&e == &*t) {
                ret = count;
            }
            ++count;
        });
    } else {
        fail();
    }
    return ret;
}

[[nodiscard]] inline std::size_t find_output_index(
    takatori::plan::step const& self,
    takatori::descriptor::relation const& target
) {
    std::size_t ret = npos;
    if (auto t = yugawara::binding::extract_if<::takatori::plan::exchange>(target)) {
        std::size_t count = 0;
        takatori::plan::enumerate_downstream(self, [&count, &t, &ret](takatori::plan::step const& e){
            if (&e == &*t) {
                ret = count;
            }
            ++count;
        });
    } else {
        fail();
    }
    return ret;
}

}

