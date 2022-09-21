#include "base128v.h"

#include <cstring>

namespace jogasaki::serializer::base128v {

using takatori::util::buffer_view;

bool write_unsigned(
        std::uint64_t value,
        buffer_view::iterator& iterator,
        buffer_view::const_iterator end) noexcept {
    using value_type = buffer_view::value_type;
    auto value_work = value;
    auto iter_work = iterator; // NOLINT(readability-qualified-auto)
    for (std::uint64_t i = 0; i < 8; ++i) {
        if (iter_work == end) {
            return false;
        }

        // the 1st ~ 8th groups have continue bit.
        // cvvv vvvv
        //   c - continue bit
        //   v - 7-bit value block
        auto group_value = value_work & 0x7fULL;

        // shift out the current group
        value_work >>= 7ULL;

        // check the rest bits
        if (value_work == 0) {
            // no more value bits
            *iter_work = static_cast<value_type>(group_value);
            ++iter_work;
            iterator = iter_work;
            return true;
        }

        // more value bits are rest
        *iter_work = static_cast<value_type>(0x80ULL | group_value);
        ++iter_work;
    }
    if (iter_work == end) {
        return false;
    }

    // the 9th group has no continue bit.
    // vvvv vvvv
    //   v - 8-bit value block
    *iter_work = static_cast<value_type>(value_work & 0xffULL);
    ++iter_work;
    iterator = iter_work;
    return true;
}

template<class Iter>
static std::optional<std::uint64_t> impl_read_unsigned(
        Iter& iterator,
        buffer_view::const_iterator end) noexcept {
    std::uint64_t result {};
    auto iter_work = iterator;
    for (std::uint64_t i = 0; i < 8; ++i) {
        if (iter_work == end) {
            return std::nullopt;
        }
        auto group = static_cast<std::uint64_t>(static_cast<unsigned char>(*iter_work));
        if (group == 0 && i != 0) {
            // for strict, all zeros group is not allowed, except just represents 0
            return std::nullopt;
        }
        ++iter_work;

        // the 1st ~ 8th groups have continue bit.
        // cvvv vvvv
        //   c - continue bit
        //   v - 7-bit value block
        result |= (group & 0x7fULL) << (i * 7);
        if ((group & 0x80ULL) == 0) {
            // end of sequence
            result |= (group & 0x7fULL) << (i * 7);
            iterator = iter_work;
            return { result };
        }
        // more groups are rest
    }
    if (iter_work == end) {
        return std::nullopt;
    }

    auto group = static_cast<std::uint64_t>(static_cast<unsigned char>(*iter_work));
    if (group == 0) {
        // for strict, all zeros group is not allowed
        return std::nullopt;
    }
    ++iter_work;

    // the 9th group has no continue bit.
    // vvvv vvvv
    //   v - 8-bit value block
    result |= (group & 0xffULL) << 56ULL;
    iterator = iter_work;
    return { result };
}

std::optional<std::uint64_t> read_unsigned(
        buffer_view::iterator& iterator,
        buffer_view::const_iterator end) noexcept {
    return impl_read_unsigned(iterator, end);
}

std::optional<std::uint64_t> read_unsigned(
        buffer_view::const_iterator& iterator,
        buffer_view::const_iterator end) noexcept {
    return impl_read_unsigned(iterator, end);
}

static std::uint64_t encode_signed(std::int64_t value) {
    std::uint64_t work {};
    std::memcpy(&work, &value, sizeof(value));
    work <<= 1ULL;
    if (value < 0) {
        work = ~work;
    }
    return work;
}

static std::int64_t decode_signed(std::uint64_t encoded) {
    std::uint64_t work = encoded;
    work >>= 1ULL;
    if ((encoded & 0x01ULL) != 0) {
        work = ~work;
    }
    std::int64_t result {};
    std::memcpy(&result, &work, sizeof(work));
    return result;
}

[[nodiscard]] size_type size_signed(std::int64_t value) {
    return size_unsigned(encode_signed(value));
}

bool write_signed(
        std::int64_t value,
        buffer_view::iterator& iterator,
        buffer_view::const_iterator end) {
    return write_unsigned(encode_signed(value), iterator, end);
}

std::optional<std::int64_t> read_signed(
        buffer_view::iterator& iterator,
        buffer_view::const_iterator end) {
    if (auto result = impl_read_unsigned(iterator, end)) {
        return { decode_signed(*result) };
    }
    return std::nullopt;
}

std::optional<std::int64_t> read_signed(
        buffer_view::const_iterator& iterator,
        buffer_view::const_iterator end) {
    if (auto result = impl_read_unsigned(iterator, end)) {
        return { decode_signed(*result) };
    }
    return std::nullopt;
}

}  // namespace jogasaki::serializer::base128v
