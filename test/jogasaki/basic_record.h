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
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_traits.h>

#include <jogasaki/executor/exchange/group/shuffle_info.h>

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

template<kind ...Kinds>
class basic_record {
public:
    basic_record() = default;
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

    std::shared_ptr<meta::record_meta> record_meta() {
        return meta_;
    }

    accessor::record_ref ref() {
        return accessor::record_ref(std::addressof(entity_), sizeof(entity_));
    }
protected:
    std::tuple<to_runtime_type_t<Kinds>...> entity_{};
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

    [[nodiscard]] value_type const& value() const noexcept {
        return std::get<1>(entity_);
    }
};

static_assert(sizeof(record) == 32);
static_assert(alignof(record) == 8);
static_assert(!std::is_trivially_copyable_v<record>);

}
