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

#include <string_view>
#include <utility>

#include <takatori/util/fail.h>
#include <sharksfin/Slice.h>
#include <sharksfin/StatusCode.h>
#include <sharksfin/api.h>

namespace jogasaki::kvs {

using takatori::util::fail;

/**
 * @brief provide kvs implementation id
 * @return kvs id such as "memory" or "shirakami"
 */
inline std::string_view implementation_id() {
    sharksfin::Slice id{};
    if (auto res = sharksfin::implementation_id(std::addressof(id)); res != sharksfin::StatusCode::OK) {
        fail();
    }
    return id.to_string_view();
}

}

