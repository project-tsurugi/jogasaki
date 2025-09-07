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

#include <jogasaki/executor/dto/common_column.h>
#include <jogasaki/proto/sql/common.pb.h>

namespace jogasaki::executor::dto {

namespace proto = jogasaki::proto;

/**
 * @brief convert proto AtomType to executor::common_column::atom_type.
 */
common_column::atom_type from(proto::sql::common::AtomType v) noexcept;

/**
 * @brief convert executor::common_column::atom_type to proto AtomType.
 */
jogasaki::proto::sql::common::AtomType from(common_column::atom_type v) noexcept;

/**
 * @brief convert proto Column message to common_column.
 */
common_column from_proto(proto::sql::common::Column const& src);

/**
 * @brief convert common_column to proto Column message.
 */
jogasaki::proto::sql::common::Column to_proto(common_column const& src);

/**
 * @brief oneof presence helpers for Column message.
 *
 * These wrappers check the generated <oneof>_case() accessor instead of
 * relying on generated has_foo() methods, which may be missing in older
 * protobuf versions.
 */
bool has_atom_type(proto::sql::common::Column const& msg) noexcept;
bool has_length(proto::sql::common::Column const& msg) noexcept;
bool has_arbitrary_length(proto::sql::common::Column const& msg) noexcept;
bool has_precision(proto::sql::common::Column const& msg) noexcept;
bool has_arbitrary_precision(proto::sql::common::Column const& msg) noexcept;
bool has_scale(proto::sql::common::Column const& msg) noexcept;
bool has_arbitrary_scale(proto::sql::common::Column const& msg) noexcept;
bool has_nullable(proto::sql::common::Column const& msg) noexcept;
bool has_varying(proto::sql::common::Column const& msg) noexcept;
bool has_description(proto::sql::common::Column const& msg) noexcept;

} // namespace jogasaki::executor
