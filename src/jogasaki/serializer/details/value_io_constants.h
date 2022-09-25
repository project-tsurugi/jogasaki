#pragma once

#include <cstdint>

#include <limits>

namespace jogasaki::serializer::details {

static constexpr std::uint32_t header_embed_positive_int = 0x00U;

static constexpr std::uint32_t header_embed_character = 0x40U;

static constexpr std::uint32_t header_embed_row = 0x80U;

static constexpr std::uint32_t header_embed_array = 0xa0U;

static constexpr std::uint32_t header_embed_negative_int = 0xc0U;

static constexpr std::uint32_t header_embed_octet = 0xd0U;

static constexpr std::uint32_t header_embed_bit = 0xe0U;

static constexpr std::uint32_t header_unknown = 0xe8U;

static constexpr std::uint32_t header_int = 0xe9U;

static constexpr std::uint32_t header_float4 = 0xeaU;

static constexpr std::uint32_t header_float8 = 0xebU;

static constexpr std::uint32_t header_decimal_compact = 0xecU;

static constexpr std::uint32_t header_decimal = 0xedU;

static constexpr std::uint32_t header_time_of_day_with_offset = 0xeeU;

static constexpr std::uint32_t header_time_point_with_offset = 0xefU;

static constexpr std::uint32_t header_character = 0xf0U;

static constexpr std::uint32_t header_octet = 0xf1U;

static constexpr std::uint32_t header_bit = 0xf2U;

static constexpr std::uint32_t header_date = 0xf3U;

static constexpr std::uint32_t header_time_of_day = 0xf4U;

static constexpr std::uint32_t header_time_point = 0xf5U;

static constexpr std::uint32_t header_datetime_interval = 0xf6U;

static constexpr std::uint32_t header_reserved_f7 = 0xf7U;

static constexpr std::uint32_t header_row = 0xf8U;

static constexpr std::uint32_t header_array = 0xf9U;

static constexpr std::uint32_t header_clob = 0xfaU;

static constexpr std::uint32_t header_blob = 0xfbU;

static constexpr std::uint32_t header_reserved_fc = 0xfcU;

static constexpr std::uint32_t header_reserved_fd = 0xfdU;

static constexpr std::uint32_t header_end_of_contents = 0xfeU;

static constexpr std::uint32_t header_reserved_ff = 0xffU;


static constexpr std::uint32_t mask_embed_positive_int = 0x3fU;

static constexpr std::uint32_t mask_embed_character = 0x3fU;

static constexpr std::uint32_t mask_embed_row = 0x1fU;

static constexpr std::uint32_t mask_embed_array = 0x1fU;

static constexpr std::uint32_t mask_embed_negative_int = 0x0fU;

static constexpr std::uint32_t mask_embed_octet = 0x0fU;

static constexpr std::uint32_t mask_embed_bit = 0x07U;


static constexpr std::int32_t min_embed_positive_int_value = 0x00;

static constexpr std::int32_t max_embed_positive_int_value = mask_embed_positive_int + min_embed_positive_int_value;


static constexpr std::int32_t max_embed_negative_int_value = -1;

static constexpr std::int32_t min_embed_negative_int_value =
        max_embed_negative_int_value - static_cast<std::int32_t>(mask_embed_negative_int);


static constexpr std::uint32_t min_embed_character_size = 0x01;

static constexpr std::uint32_t max_embed_character_size = mask_embed_character + min_embed_character_size;


static constexpr std::uint32_t min_embed_octet_size = 0x01;

static constexpr std::uint32_t max_embed_octet_size = mask_embed_octet + min_embed_octet_size;


static constexpr std::uint32_t min_embed_bit_size = 0x01;

static constexpr std::uint32_t max_embed_bit_size = mask_embed_bit + min_embed_bit_size;


static constexpr std::uint32_t min_embed_row_size = 0x01;

static constexpr std::uint32_t max_embed_row_size = mask_embed_row + min_embed_row_size;


static constexpr std::uint32_t min_embed_array_size = 0x01;

static constexpr std::uint32_t max_embed_array_size = mask_embed_array + min_embed_array_size;

static constexpr std::uint32_t limit_size = static_cast<std::uint32_t>(std::numeric_limits<std::int32_t>::max()) + 1UL;


static constexpr std::uint64_t max_decimal_coefficient_size = sizeof(std::uint64_t) * 2 + 1;

} // namespace jogasaki::serializer::details
