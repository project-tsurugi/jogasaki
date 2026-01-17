/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <jogasaki/lob/blob_reference.h>
#include <jogasaki/lob/clob_reference.h>
#include <jogasaki/utils/fail.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>

namespace jogasaki::udf::data {

takatori::decimal::triple decode_decimal_triple(std::string const& unscaled, std::int32_t exponent);
takatori::datetime::date decode_date_from_wire(std::int32_t days);
takatori::datetime::time_of_day decode_time_of_day_from_wire(std::int64_t nanos);
takatori::datetime::time_point decode_time_point_from_wire(
    std::int64_t seconds, std::uint32_t nanos);
jogasaki::lob::blob_reference decode_blob_reference_from_wire(std::uint64_t storage_id,
    std::uint64_t object_id, std::uint64_t tag, std::optional<bool> provisioned);
jogasaki::lob::clob_reference decode_clob_reference_from_wire(std::uint64_t storage_id,
    std::uint64_t object_id, std::uint64_t tag, std::optional<bool> provisioned);
} // namespace jogasaki::udf::data
