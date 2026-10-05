/* Copyright 2026 Project Tsurugi. Licensed under the Apache License, Version 2.0. */
#pragma once
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

#include <jogasaki/accessor/text.h>
namespace jogasaki::accessor {
// Canonical, immutable sequence of '0' and '1'; storage follows text's resource lifetime.
class bit {
public:
    bit() = default;
    explicit bit(memory::paged_memory_resource* resource, std::string_view value) : value_(make_text(resource, value)) {}
    bit(memory::paged_memory_resource* resource, bit value) : value_(resource, value.value_) {}
    [[nodiscard]] std::size_t size() const noexcept { return value_.size(); }
    [[nodiscard]] explicit operator std::string_view() const & noexcept { return static_cast<std::string_view>(value_); }
    explicit operator std::string_view() && = delete;
    friend bool operator==(bit const& a, bit const& b) noexcept { return a.value_ == b.value_; }
    friend bool operator!=(bit const& a, bit const& b) noexcept { return !(a == b); }
    friend std::ostream& operator<<(std::ostream& out, bit const& value) { return out << value.value_; }
private:
    static text make_text(memory::paged_memory_resource* resource, std::string_view value) {
        if (!resource && value.size() > 15) throw std::invalid_argument("long BIT literal requires a memory resource");
        return resource ? text{resource, value} : text{value};
    }
    text value_{};
};
static_assert(std::is_trivially_copyable_v<bit>);
}
