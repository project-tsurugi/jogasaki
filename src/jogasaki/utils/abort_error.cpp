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
#include "abort_error.h"

#include <takatori/util/string_builder.h>

#include <jogasaki/index/utils.h>
#include <jogasaki/index/index_accessor.h>
#include <jogasaki/index/field_factory.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/accessor/record_printer.h>

namespace jogasaki::utils {

using takatori::util::string_builder;

std::pair <maybe_shared_ptr<meta::record_meta>, accessor::record_ref>
read_key_as_record_ref(const yugawara::storage::configurable_provider &tables, data::aligned_buffer &buf,
    std::string_view storage_name, std::string_view data, memory::lifo_paged_memory_resource *resource) {
    auto idx = find_storage(tables, storage_name);
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
            auto [meta, ref] = read_key_as_record_ref(tables, buf, loc->storage(), loc->key(), resource);
            ss << "location={key:";
            if(meta) {
                ss << ref << *meta;
            } else {
                auto d = loc->key();
                utils::binary_printer p{d.data(), d.size()};
                ss << p;
            }
            ss << " ";
            ss << "storage:" << loc->storage();
            ss << "}";
        }
        default: break;
    }
}

std::string create_abort_message(request_context &rctx, transaction_context &tx,
    const yugawara::storage::configurable_provider &tables) {
    auto result = tx.object()->recent_call_result();
    std::string_view desc{};
    std::stringstream ss{};
    if(result) {
        desc = result->description();
        handle_code_and_locator(result->code(), result->location().get(), tables, rctx.request_resource(), ss);
    }
    std::string idstr{};
    if(auto txid = tx.object()->transaction_id(); ! txid.empty()) {
        idstr = "transaction:" + std::string{txid} + " ";
    }

    return string_builder{} << "Transaction aborted. " << idstr << desc << " " << ss.str() << string_builder::to_string;
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

