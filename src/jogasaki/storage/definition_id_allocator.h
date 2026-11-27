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
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/status.h>

namespace jogasaki::storage {
// IDs belong to a worker database, not to a process or a live-storage list.
class definition_id_allocator {
public:
    static constexpr char storage_name[] = "__system_storage_definition_ids";
    explicit definition_id_allocator(kvs::database& db, std::chrono::milliseconds durability_wait = std::chrono::seconds(30)) :
        db_(&db), durability_wait_(durability_wait) {}
    status initialize();
    status discard_created_storage();
    status inspect(bool& initialized);
    status reserve(std::uint64_t count, std::uint64_t minimum, std::uint64_t& first);
    // Assign only missing IDs in the serialized metadata, preserving prepared statements.
    status assign(std::string& payload);
    [[nodiscard]] bool storage_created() const noexcept { return storage_created_; }
    [[nodiscard]] std::shared_ptr<std::atomic<std::uint64_t>> const& durable_marker() const { return durable_; }
private:
    kvs::database* db_{};
    std::chrono::milliseconds durability_wait_{};
    std::unique_ptr<kvs::storage> storage_{};
    bool storage_created_{};
    std::shared_ptr<std::atomic<std::uint64_t>> durable_{std::make_shared<std::atomic<std::uint64_t>>(0)};
};
}
