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
#include "definition_id_allocator.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <string_view>
#include <thread>
#include <sharksfin/api.h>
#include <jogasaki/kvs/error.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/utils/surrogate_id_utils.h>

namespace jogasaki::storage {
status definition_id_allocator::initialize() {
    storage_ = db_->get_storage(storage_name);
    if (!storage_) storage_ = db_->create_storage(storage_name);
    return storage_ ? status::ok : status::err_io_error;
}

status definition_id_allocator::reserve(std::uint64_t count, std::uint64_t minimum, std::uint64_t& first) {
    if (!storage_) return status::err_invalid_state;
    // Only conflicting metadata transactions retry; IDs from successful reservations never roll back.
    for (std::size_t attempt = 0; attempt < 128; ++attempt) {
        std::unique_ptr<kvs::transaction> tx{};
        auto rc = kvs::transaction::create_transaction(*db_, tx);
        if (rc != status::ok) return rc;
        std::string_view value{};
        rc = storage_->content_get(*tx, "next", value);
        std::uint64_t next = 1;
        bool missing = rc == status::not_found;
        if (rc == status::ok) {
            if (value.size() != sizeof(next)) return status::err_invalid_state;
            next = utils::from_big_endian(value);
            if (next == 0) return status::err_invalid_state;
        } else if (rc != status::not_found && rc != status::concurrent_operation && rc != status::err_serialization_failure) {
            return rc;
        }
        if (rc == status::concurrent_operation || rc == status::err_serialization_failure) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        // This marker commits atomically with the counter. Empty storage can be
        // left by a crash before its first transaction; a seeded counter cannot reset.
        std::string_view initialized{};
        rc = storage_->content_get(*tx, "initialized", initialized);
        if (rc == status::concurrent_operation || rc == status::err_serialization_failure) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        if (rc != status::ok && rc != status::not_found) return rc;
        if (rc == status::ok && (initialized != "v1" || missing)) return status::err_invalid_state;
        next = std::max(next, minimum);
        if (count > std::numeric_limits<std::uint64_t>::max() - next) return status::err_resource_limit_reached;
        auto bytes = utils::to_big_endian(next + count);
        rc = storage_->content_put(*tx, "next", bytes);
        if (rc == status::ok) rc = storage_->content_put(*tx, "initialized", "v1");
        if (rc == status::ok) {
            std::atomic_bool done{false};
            std::uint64_t marker{};
            (void) tx->commit([&](sharksfin::StatusCode result, sharksfin::ErrorCode, sharksfin::durability_marker_type m) {
                rc = kvs::resolve(result);
                marker = m;
                done.store(true, std::memory_order_release);
            });
            while (!done.load(std::memory_order_acquire)) std::this_thread::sleep_for(std::chrono::milliseconds(1));
            if (rc == status::ok) {
                // Do not publish an ID until its reservation survives a worker crash.
                auto deadline = std::chrono::steady_clock::now() + durability_wait_;
                while (durable_->load(std::memory_order_acquire) < marker) {
                    if (std::chrono::steady_clock::now() >= deadline) return status::err_time_out;
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                first = next;
                return status::ok;
            }
        }
        if (rc != status::concurrent_operation && rc != status::err_serialization_failure) return rc;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return status::err_serialization_failure;
}

status definition_id_allocator::assign(std::string& payload) {
    proto::metadata::storage::Storage metadata{};
    if (!metadata.ParseFromString(payload) || !metadata.has_index()) return status::err_invalid_state;
    auto& index = *metadata.mutable_index();
    std::uint64_t minimum = 1;
    auto advance = [&](std::uint64_t id) {
        minimum = std::max(minimum, id == std::numeric_limits<std::uint64_t>::max() ? id : id + 1);
    };
    bool missing_index = index.definition_id_optional_case() == proto::metadata::storage::IndexDefinition::DEFINITION_ID_OPTIONAL_NOT_SET;
    bool missing_table = index.has_table_definition() && index.table_definition().definition_id_optional_case() == proto::metadata::storage::TableDefinition::DEFINITION_ID_OPTIONAL_NOT_SET;
    if (!missing_index) advance(index.definition_id());
    if (index.has_table_definition() && !missing_table) advance(index.table_definition().definition_id());
    std::uint64_t first{};
    if (auto rc = reserve(static_cast<std::uint64_t>(missing_index) + static_cast<std::uint64_t>(missing_table), minimum, first); rc != status::ok) return rc;
    if (missing_table) index.mutable_table_definition()->set_definition_id(first++);
    if (missing_index) index.set_definition_id(first);
    return metadata.SerializeToString(&payload) ? status::ok : status::err_invalid_state;
}
}
