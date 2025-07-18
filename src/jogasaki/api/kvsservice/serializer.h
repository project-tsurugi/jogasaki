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
#include <vector>

#include <takatori/type/data.h>
#include <takatori/type/type_kind.h>
#include <yugawara/storage/column.h>
#include <tateyama/proto/kvs/data.pb.h>
#include <sharksfin/api.h>

#include <jogasaki/api/kvsservice/status.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/serializer/value_input.h>

#include "column_data.h"

namespace jogasaki::api::kvsservice {

/**
 * @brief get the buffer length required to serialize the values.
 * This method just calculates the buffer size to required to serialize,
 * doesn't really serialize the values. Use serialize() to serialize really.
 * @param spec the coding_spec to serialize the values
 * @param list the list of values to be serialized
 * @param size [out] the buffer length required to serialize the values
 * @return status::ok if succeeded
 * @return otherwise if error was occurred
 */
status get_bufsize(jogasaki::kvs::coding_spec const &spec, std::vector<column_data> const &list, std::size_t &size);

/**
 * @brief serialize the values
 * @param spec the coding_spec to serialize the values
 * @param list the list of values to be serialized
 * @param results [out]the output buffer to write serialized data
 * @return status::ok if succeeded
 * @return otherwise if error was occurred
 */
status serialize(jogasaki::kvs::coding_spec const &spec, std::vector<column_data> const &list,
                 jogasaki::kvs::writable_stream &results);

/**
 * @brief deserialize the value
 * @param spec the coding_spec to serialize the values
 * @param column column schema data
 * @param stream the input buffer to read serialized data
 * @param value [out]the output value
 * @return status::ok if succeeded
 * @return otherwise if error was occurred
 */
status deserialize(jogasaki::kvs::coding_spec const &spec, yugawara::storage::column const &column,
                   jogasaki::kvs::readable_stream &stream, tateyama::proto::kvs::data::Value *value);

}
