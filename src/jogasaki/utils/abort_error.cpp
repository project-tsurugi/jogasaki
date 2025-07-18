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
#include "abort_error.h"

#include <optional>
#include <ostream>
#include <type_traits>
#include <utility>
#include <boost/assert.hpp>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/string_builder.h>
#include <sharksfin/CallResult.h>
#include <sharksfin/ErrorCode.h>
#include <sharksfin/ErrorLocator.h>

#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/constants.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/index/field_factory.h>
#include <jogasaki/index/index_accessor.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/request_context.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/binary_printer.h>

namespace jogasaki::utils {

using takatori::util::string_builder;

std::pair <maybe_shared_ptr<meta::record_meta>, accessor::record_ref>
read_key_as_record_ref(const yugawara::storage::configurable_provider &tables, data::aligned_buffer &buf,
    std::string_view storage_name, std::string_view data, memory::lifo_paged_memory_resource *resource) {
    auto idx = find_storage(tables, storage_name);
    if(! idx) {
        // Storage name is not available somehow. Return empty metadata and let caller handle this case.
        return {};
    }
    auto meta = index::create_meta(*idx, true);
    auto mapper = std::make_shared<index::mapper>(
        index::index_fields(*idx, true),
        index::index_fields(*idx, false)
    );
    kvs::readable_stream stream{data.data(), data.size()};
    auto sz = meta->record_size();
    buf.resize(sz);
    accessor::record_ref rec{buf.data(), sz};
    if(! mapper->read(true, stream, rec, resource)) {
        // key is corrupted or not available
        return {};
    }
    return {std::move(meta), rec};
}

void handle_code_and_locator(sharksfin::ErrorCode code, sharksfin::ErrorLocator *locator,
    const yugawara::storage::configurable_provider &tables, memory::lifo_paged_memory_resource *resource,
    std::ostream &ss) {
    if(locator == nullptr) return;
    using ErrorCode = sharksfin::ErrorCode;
    switch(code) {
        case ErrorCode::KVS_KEY_ALREADY_EXISTS: // fall-thru
        case ErrorCode::KVS_KEY_NOT_FOUND: // fall-thru
        case ErrorCode::CC_LTX_WRITE_ERROR: // fall-thru
        case ErrorCode::CC_OCC_READ_ERROR: {
            BOOST_ASSERT(locator->kind() == sharksfin::ErrorLocatorKind::storage_key); //NOLINT
            auto loc = static_cast<sharksfin::StorageKeyErrorLocator*>(locator);  //NOLINT
            data::aligned_buffer buf{default_record_buffer_size};
            auto stg = loc->storage().has_value() ? loc->storage().value() : "";
            ss << "location={key:";
            if (loc->key().has_value()) {
                auto [meta, ref] = read_key_as_record_ref(tables, buf, stg, loc->key().value(), resource);
                if (meta) {
                    ss << ref << *meta;
                } else {
                    auto d = loc->key().value();
                    utils::binary_printer p{d.data(), d.size()};
                    ss << p;
                }
            } else {
                ss << "<not available>";
            }
            ss << " storage:";
            if (loc->storage().has_value()) {
                ss << loc->storage().value();
            } else {
                ss << "<not available>";
            }
            ss << "}";
        }
        default: break;
    }
}

std::string transaction_id_str(transaction_context& tx) {
    if(auto txid = tx.transaction_id(); ! txid.empty()) {
        return "transaction:" + std::string{txid} + " ";
    }
    return {};
}

std::string create_abort_message(
    request_context const& rctx
) {
    auto& tx = *rctx.transaction();
    auto& tables = rctx.storage_provider();
    auto result = tx.recent_call_result();
    std::string_view desc{};
    std::stringstream ss{};
    if(result) {
        desc = result->description();
        if(tables) {
            handle_code_and_locator(result->code(), result->location().get(), *tables, rctx.request_resource(), ss);
        }
    }
    auto idstr = transaction_id_str(tx);
    return string_builder{} << "serialization failed " << idstr << desc << " " << ss.str() << string_builder::to_string;
}

std::shared_ptr<yugawara::storage::index const>
find_storage(const yugawara::storage::configurable_provider &tables, std::string_view storage_name) {
    std::shared_ptr<yugawara::storage::index const> found{};
    tables.each_index([&](std::string_view id, std::shared_ptr<yugawara::storage::index const> const& entry) {
        (void) id;
        if (entry->simple_name() == storage_name) {
            found = entry;
        }
    });
    return found;
}

}

