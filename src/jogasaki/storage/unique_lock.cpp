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
#include "unique_lock.h"

#include <memory>
#include <string>
#include <string_view>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>
#include <jogasaki/storage/storage_list.h>
#include <jogasaki/storage/storage_manager.h>

namespace jogasaki::storage {

unique_lock::~unique_lock() noexcept {
    for(auto&& e : storages_.entries()) {
        try {
            auto s = manager_->find_entry(e);
            if (s) {
                s->release();
            }
        } catch (...) {
            LOG_LP(ERROR) << "unexpected exception occurred while releasing storage:" << e;
        }
    }
}

unique_lock::unique_lock(storage_manager& manager, storage_list storages) :
    manager_(std::addressof(manager)),
    storages_(std::move(storages))
{}

storage_list_view unique_lock::storage() {
    return storages_;
}

} // namespace jogasaki::storage