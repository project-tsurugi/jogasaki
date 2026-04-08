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
#include <cstdint>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/assert.h>

namespace jogasaki::utils {

inline void assert_all_fields_nullable(meta::record_meta const& meta) {
    for(std::size_t i=0, n = meta.field_count(); i < n; ++i) {
        (void)i;
        assert_with_exception(meta.nullable(i));
    }
}

} //namespace

