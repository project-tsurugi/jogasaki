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
#include <vector>

#include <yugawara/storage/index.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/status.h>

namespace jogasaki::index {

/**
 * @brief decode fields
 * @param fields fields to decode
 * @param stream input stream to decode
 * @param target target record
 * @param resource memory resource for decoding
 * @return status::ok when successful
 * @return status::err_data_corruption if decoded data is not valid
 * @return any error otherwise
 */
status decode_fields(
    std::vector<index::field_info> const& fields,
    kvs::readable_stream& stream,
    accessor::record_ref target,
    memory::lifo_paged_memory_resource* resource
);

class mapper {
public:
    mapper() = default;

    mapper(
        std::vector<field_info> key_fields,
        std::vector<field_info> value_fields
    );

    bool read(
        bool key,
        kvs::readable_stream& stream,
        accessor::record_ref target,
        memory::lifo_paged_memory_resource* resource
    );

private:
    std::vector<field_info> key_fields_{};
    std::vector<field_info> value_fields_{};
};

}


