/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <string_view>
#include <vector>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/mock/basic_record.h>

namespace jogasaki::utils {

void set_null(accessor::record_ref ref, std::size_t index, meta::record_meta& meta);

std::vector<mock::basic_record> deserialize_msg(
    std::string_view data,
    jogasaki::meta::record_meta& meta
);

std::vector<mock::basic_record> deserialize_msg(
    std::vector<std::string_view> const& data,
    jogasaki::meta::record_meta& meta
);

}
