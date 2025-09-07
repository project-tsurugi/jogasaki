/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <jogasaki/executor/dto/describe_table.h>
#include <jogasaki/proto/sql/response.pb.h>

namespace jogasaki::executor::dto {

namespace proto = jogasaki::proto;

/**
 * @brief convert proto DescribeTable::Success to describe_table
 */
describe_table from_proto(proto::sql::response::DescribeTable::Success const& src);

/**
 * @brief convert describe_table to proto DescribeTable::Success
 */
proto::sql::response::DescribeTable::Success to_proto(describe_table const& src);

} // namespace jogasaki::executor
