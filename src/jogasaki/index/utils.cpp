/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include "utils.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <boost/move/utility_core.hpp>

#include <takatori/type/character.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <yugawara/storage/column.h>

#include <jogasaki/kvs/coder.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::index {

using takatori::util::maybe_shared_ptr;

maybe_shared_ptr<meta::record_meta> create_meta(yugawara::storage::index const& idx, bool for_key) {
    std::vector<meta::field_type> types{};
    boost::dynamic_bitset<std::uint64_t> nullities{};
    if (for_key) {
        for(auto&& k : idx.keys()) {
            types.emplace_back(utils::type_for(k.column().type()));
            nullities.push_back(true);
        }
    } else {
        for(auto&& v : idx.values()) {
            types.emplace_back(utils::type_for(static_cast<yugawara::storage::column const&>(v).type()));
            nullities.push_back(true);
        }
    }
    return std::make_shared<meta::record_meta>(std::move(types), std::move(nullities));
}

kvs::storage_spec extract_storage_spec(takatori::type::data const& type) {
    if(type.kind() == takatori::type::type_kind::character) {
        auto& ct = takatori::util::unsafe_downcast<takatori::type::character>(type);
        auto varying = ct.varying();
        auto len = ct.length() ? *ct.length() : (varying ? system_varchar_default_length : system_char_default_length);
        return {!varying, len};
    }
    return {};
}

}
