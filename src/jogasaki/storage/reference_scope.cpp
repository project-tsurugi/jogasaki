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
#include <jogasaki/storage/reference_scope.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/storage/storage_manager.h>

namespace jogasaki::storage {

reference_scope::~reference_scope() noexcept {
    for (auto&& e : storages_) {
        try {
            auto s = manager_->find_entry(e);
            if (s) {
                s->decrement_ref_transaction_count();
            }
        } catch (...) {
            LOG_LP(ERROR) << "unexpected exception occurred while releasing storage reference:" << e;
        }
    }
}

reference_scope::reference_scope(storage_manager& manager) :
    manager_(std::addressof(manager))
{}

void reference_scope::add_storage(storage_entry entry) {
    std::scoped_lock lock{mutex_};
    if (storages_.find(entry) != storages_.end()) {
        return;
    }
    auto s = manager_->find_entry(entry);
    if (s) {
        s->increment_ref_transaction_count();
        storages_.insert(entry);
    }
}

} // namespace jogasaki::storage
