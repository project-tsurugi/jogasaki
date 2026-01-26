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
#include "udf_wire_codec.h"

#include <jogasaki/lob/blob_reference.h>
#include <jogasaki/lob/clob_reference.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
namespace jogasaki::udf::data {
namespace {
template <class Ref>
Ref decode_lob_reference_from_wire(
    std::uint64_t storage_id,
    std::uint64_t object_id,
    std::uint64_t tag,
    std::optional<bool> provisioned
) {
    using provider = jogasaki::lob::lob_data_provider;

    if (storage_id == 1ULL) {
        if (provisioned && provisioned.value()) {
            return Ref{object_id, provider::datastore}.reference_tag(tag);
        }
        return Ref{object_id}.reference_tag(tag);
    }
    if (storage_id == 0ULL) {
        return Ref{object_id, provider::relay_service_session}.reference_tag(tag);
    }
    fail_with_exception_msg("invalid storage_id for lob_reference");
}
} // namespace anonymous
takatori::decimal::triple decode_decimal_triple(
    std::string const& unscaled, std::int32_t exponent) {
    bool negative            = false;
    unsigned __int128 ucoeff = 0;

    bool is_negative =
        (!unscaled.empty()) && ((static_cast<unsigned char>(unscaled[0]) & 0x80U) != 0U);

    if (is_negative) {
        negative = true;
        std::vector<std::uint8_t> bytes;
        bytes.reserve(unscaled.size());

        for (unsigned char c : unscaled) {
            bytes.emplace_back(static_cast<std::uint8_t>(c));
        }

        for (auto& b : bytes) {
            // NOLINTNEXTLINE(hicpp-signed-bitwise)
            b = static_cast<std::uint8_t>(~b);
        }

        for (int i = static_cast<int>(bytes.size()) - 1; i >= 0; --i) {
            if (++bytes[i] != 0) break;
        }

        for (std::uint8_t b : bytes) {
            // NOLINTNEXTLINE(hicpp-signed-bitwise)
            ucoeff = (ucoeff << 8) | b;
        }
    } else {
        for (unsigned char b : unscaled) {
            // NOLINTNEXTLINE(hicpp-signed-bitwise)
            ucoeff = (ucoeff << 8) | static_cast<std::uint8_t>(b);
        }
    }

    // NOLINTNEXTLINE(hicpp-signed-bitwise)
    auto coeff_high = static_cast<std::uint64_t>((ucoeff >> 64) & 0xFFFFFFFFFFFFFFFFULL);

    // NOLINTNEXTLINE(hicpp-signed-bitwise)
    auto coeff_low = static_cast<std::uint64_t>(ucoeff & 0xFFFFFFFFFFFFFFFFULL);

    std::int64_t sign = negative ? -1 : (ucoeff == 0 ? 0 : +1);

    return {sign, coeff_high, coeff_low, exponent};
}
takatori::datetime::date decode_date_from_wire(std::int32_t days) {
    return takatori::datetime::date{static_cast<takatori::datetime::date::difference_type>(days)};
}
takatori::datetime::time_of_day decode_time_of_day_from_wire(std::int64_t nanos) {
    return takatori::datetime::time_of_day{
        static_cast<takatori::datetime::time_of_day::time_unit>(nanos)};
}
takatori::datetime::time_point decode_time_point_from_wire(
    std::int64_t seconds, std::uint32_t nanos) {
    return takatori::datetime::time_point{
        takatori::datetime::time_point::offset_type(seconds), std::chrono::nanoseconds(nanos)};
}
jogasaki::lob::blob_reference decode_blob_reference_from_wire(std::uint64_t storage_id,
    std::uint64_t object_id, std::uint64_t tag, std::optional<bool> provisioned) {
    return decode_lob_reference_from_wire<jogasaki::lob::blob_reference>(
        storage_id, object_id, tag, provisioned);
}

jogasaki::lob::clob_reference decode_clob_reference_from_wire(std::uint64_t storage_id,
    std::uint64_t object_id, std::uint64_t tag, std::optional<bool> provisioned) {
    return decode_lob_reference_from_wire<jogasaki::lob::clob_reference>(
        storage_id, object_id, tag, provisioned);
}

} // namespace jogasaki::udf::data
