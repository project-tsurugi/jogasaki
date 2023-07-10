/*
 * Copyright 2018-2023 tsurugi project.
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

#include <vector>

#include <tateyama/proto/kvs/data.pb.h>
#include <jogasaki/api/kvsservice/status.h>
#include <sharksfin/api.h>
#include <jogasaki/serializer/value_input.h>
#include <takatori/type/data.h>
#include <takatori/type/type_kind.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>

namespace jogasaki::api::kvsservice {

status serialize(jogasaki::kvs::coding_spec const &spec, bool nullable,
                 std::vector<tateyama::proto::kvs::data::Value const*> &values,
                 jogasaki::kvs::writable_stream &results);

status deserialize(jogasaki::kvs::coding_spec const &spec, bool nullable, takatori::type::data const &data,
                   jogasaki::kvs::readable_stream &stream, tateyama::proto::kvs::data::Value *value);

}
