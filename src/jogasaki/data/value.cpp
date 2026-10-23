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
#include "value.h"
#include <string>

namespace jogasaki::data {
value value::copy(any source) {
    if (source.type_index() == any::index<accessor::text>) {
        auto text = source.to<accessor::text>();
        if (text.is_short()) return {source, {}};
        auto owner = std::make_shared<std::string const>(static_cast<std::string_view>(text));
        return {any{std::in_place_type<accessor::text>, accessor::text{std::string_view{*owner}}}, owner};
    }
    if (source.type_index() == any::index<accessor::binary>) {
        auto bytes = source.to<accessor::binary>();
        if (bytes.is_short()) return {source, {}};
        auto owner = std::make_shared<std::string const>(static_cast<std::string_view>(bytes));
        return {any{std::in_place_type<accessor::binary>, accessor::binary{std::string_view{*owner}}}, owner};
    }
    if (source.type_index() == any::index<lob::blob_reference>) {
        auto ref = source.to<lob::blob_reference>();
        if (ref.locator()) {
            auto owner = std::make_shared<lob::lob_locator const>(*ref.locator());
            auto copied = lob::blob_reference{*owner};
            copied.reference_tag(static_cast<lob::lob_reference const&>(ref).reference_tag());
            return {any{std::in_place_type<lob::blob_reference>, copied}, owner};
        }
    }
    if (source.type_index() == any::index<lob::clob_reference>) {
        auto ref = source.to<lob::clob_reference>();
        if (ref.locator()) {
            auto owner = std::make_shared<lob::lob_locator const>(*ref.locator());
            auto copied = lob::clob_reference{*owner};
            copied.reference_tag(static_cast<lob::lob_reference const&>(ref).reference_tag());
            return {any{std::in_place_type<lob::clob_reference>, copied}, owner};
        }
    }
    return {source, {}};
}
std::string_view value::bytes() const & {
    return value_.visit([](auto const& item) -> std::string_view {
        using T = std::decay_t<decltype(item)>;
        if constexpr (std::is_same_v<T, accessor::text> || std::is_same_v<T, accessor::binary>) {
            return static_cast<std::string_view>(item);
        } else fail_with_exception();
    });
}
value value::from_blob(lob::blob_locator locator) {
    auto owner = std::make_shared<lob::blob_locator const>(std::move(locator));
    return {any{std::in_place_type<lob::blob_reference>, lob::blob_reference{*owner}}, owner};
}
value value::from_clob(lob::clob_locator locator) {
    auto owner = std::make_shared<lob::clob_locator const>(std::move(locator));
    return {any{std::in_place_type<lob::clob_reference>, lob::clob_reference{*owner}}, owner};
}
}
