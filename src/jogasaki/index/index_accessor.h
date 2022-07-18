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

#include <cstddef>

#include <yugawara/storage/index.h>

#include <jogasaki/index/field_info.h>

namespace jogasaki::index {

class mapper {
public:
    void read(bool key, kvs::readable_stream& stream, accessor::record_ref target) {
        (void) key;
        (void) stream;
        (void) target;
    }
    void write(bool key, accessor::record_ref src, kvs::writable_stream& stream) {
        (void) key;
        (void) stream;
        (void) src;
    }
private:
    std::vector<field_info> key_fields_{};
    std::vector<field_info> value_fields_{};
};

}


