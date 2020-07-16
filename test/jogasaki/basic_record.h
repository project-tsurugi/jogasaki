/*
 * Copyright 2018-2020 tsurugi project.
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

#include <tuple>
#include <sstream>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_traits.h>

#include <jogasaki/executor/exchange/group/shuffle_info.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/accessor/record_printer.h>

namespace jogasaki::testing {

using kind = meta::field_type_kind;

template<kind Kind>
struct to_runtime_type {
    using type = typename meta::field_type_traits<Kind>::runtime_type;
};

template<kind Kind>
using to_runtime_type_t = typename to_runtime_type<Kind>::type;

template <size_t I, kind ...Kinds>
std::size_t offset_at(std::tuple<to_runtime_type_t<Kinds>...>& entity) {
    return static_cast<char*>(static_cast<void*>(std::addressof(std::get<I>(entity)))) - static_cast<char*>(static_cast<void*>(std::addressof(entity)));
}

template <kind ...Kinds, size_t ... Is>
std::vector<std::size_t> offsets(std::tuple<to_runtime_type_t<Kinds>...>& entity, std::index_sequence<Is...>) {
    return {offset_at<Is, Kinds...>(entity)...};
}

template <kind ...Kinds>
std::vector<std::size_t> offsets(std::tuple<to_runtime_type_t<Kinds>...>& entity) {
    return offsets<Kinds...>(entity, std::make_index_sequence<sizeof...(Kinds)>());
}

template <kind ...Kinds, size_t ... Is>
std::tuple<to_runtime_type_t<Kinds>...> values(accessor::record_ref ref, meta::record_meta& meta, std::index_sequence<Is...>) {
    return std::tuple<to_runtime_type_t<Kinds>...>{ ref.get_value<to_runtime_type_t<Kinds>>(meta.value_offset(Is))...};
}

template <kind ...Kinds>
std::tuple<to_runtime_type_t<Kinds>...> values(accessor::record_ref ref, meta::record_meta& meta) {
    return values<Kinds...>(ref, meta, std::make_index_sequence<sizeof...(Kinds)>());
}

template<kind ...Kinds>
class basic_record {
public:
    using entity_type = std::tuple<to_runtime_type_t<Kinds>...>;

    basic_record() {
        meta_ = std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{meta::field_type(takatori::util::enum_tag<Kinds>)...},
            boost::dynamic_bitset<std::uint64_t>{sizeof...(Kinds)},  // all fields non-nullable
            std::vector<std::size_t>{offsets<Kinds...>(entity_)},
            std::vector<std::size_t>{(void(Kinds), 0)...},
            alignof(std::tuple<to_runtime_type_t<Kinds>...>),
            sizeof(entity_type)
        );
    }

    /**
     * @brief construct new object with non-nullable fields
     */
    template <typename T = std::enable_if<sizeof...(Kinds) != 0, void>>
    explicit basic_record(to_runtime_type_t<Kinds>...args) : entity_(args...) {
        meta_ = std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{meta::field_type(takatori::util::enum_tag<Kinds>)...},
            boost::dynamic_bitset<std::uint64_t>{sizeof...(args)},  // all fields non-nullable
            std::vector<std::size_t>{offsets<Kinds...>(entity_)},
        std::vector<std::size_t>{(void(args), 0)...},
        alignof(std::tuple<to_runtime_type_t<Kinds>...>),
        sizeof(entity_)
        );
    }

    /**
     * @brief construct new objects with given nullability offsets
     */
    explicit basic_record(boost::dynamic_bitset<std::uint64_t> nullability,
        std::vector<std::size_t> nullity_offset_table,
        to_runtime_type_t<Kinds>...args) : entity_(args...) {
        meta_ = std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{meta::field_type(takatori::util::enum_tag<Kinds>)...},
            std::move(nullability),
            std::vector<std::size_t>{offsets<Kinds...>(entity_)},
            std::move(nullity_offset_table),
            alignof(std::tuple<to_runtime_type_t<Kinds>...>),
            sizeof(entity_)
        );
    }

    explicit basic_record(accessor::record_ref ref) : basic_record() {
        entity_ = values<Kinds...>(ref, *meta_);
    }

    [[nodiscard]] std::shared_ptr<meta::record_meta> const& record_meta() const noexcept {
        return meta_;
    }

    [[nodiscard]] accessor::record_ref ref() const noexcept {
        return accessor::record_ref(const_cast<entity_type*>(std::addressof(entity_)), sizeof(entity_)); //FIXME
    }

    /// @brief equality comparison operator
    friend bool operator==(basic_record<Kinds...> const& a, basic_record<Kinds...> const& b) noexcept {
        return a.entity_ == b.entity_;
    }

    /// @brief inequality comparison operator
    friend bool operator!=(basic_record<Kinds...> const& a, basic_record<Kinds...> const& b) noexcept {
        return !(a == b);
    }

    friend std::ostream& operator<<(std::ostream& out, basic_record<Kinds...> const& value) {
        auto& v = const_cast<basic_record<Kinds...>&>(value);
        std::stringstream ss{};
        ss << v.ref() << *v.meta_;
        return out << ss.str();
    }

protected:
    entity_type entity_{};
    std::shared_ptr<meta::record_meta> meta_{};
};


class record : public basic_record<kind::int8, kind::float8> {
public:
    using key_type = std::int64_t;
    using value_type = double;

    record() noexcept : basic_record(0, 0.0) {};

    record(key_type key, value_type value) : basic_record(key, value) {}

    [[nodiscard]] key_type const& key() const noexcept {
        return std::get<0>(entity_);
    }

    void key(key_type arg) noexcept {
        std::get<0>(entity_) = arg;
    }

    [[nodiscard]] value_type const& value() const noexcept {
        return std::get<1>(entity_);
    }

    void value(value_type arg) noexcept {
        std::get<1>(entity_) = arg;
    }
};

static_assert(sizeof(record) == 32);
static_assert(alignof(record) == 8);
static_assert(!std::is_trivially_copyable_v<record>);

class record_f4f8ch : public basic_record<kind::float8, kind::int4, kind::character> {
public:
    using key_type = std::int32_t;
    using f4_value_type = double;
    using ch_value_type = meta::field_type_traits<kind::character>::runtime_type;

    record_f4f8ch() noexcept : basic_record(0.0, 0, ch_value_type{}) {};

    record_f4f8ch(f4_value_type f4_value, key_type key, ch_value_type ch_value) : basic_record(f4_value, key, ch_value) {}

    [[nodiscard]] key_type const& key() const noexcept {
        return std::get<1>(entity_);
    }

    [[nodiscard]] f4_value_type const& f4_value() const noexcept {
        return std::get<0>(entity_);
    }

    [[nodiscard]] ch_value_type const& ch_value() const noexcept {
        return std::get<2>(entity_);
    }
};

}
