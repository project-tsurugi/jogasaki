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
#include <memory>

#include "storage_context.h"
#include "transaction_context.h"

namespace jogasaki::storage {

storage_context::~storage_context() noexcept {
    if (db_) {
        sharksfin::database_dispose(db_);
    }
}

bool storage_context::open(std::map<std::string, std::string> const& options) {
    sharksfin::DatabaseOptions dbopts{};
    for(auto& p : options) {
        dbopts.attribute(p.first, p.second);
    }
    if(auto res = sharksfin::database_open(dbopts, &db_); res != sharksfin::StatusCode::OK) {
        LOG(ERROR) << "database_open failed with " << res;
        return false;
    }
    return true;
}

bool storage_context::close() {
    if(auto res = sharksfin::database_close(db_); res != sharksfin::StatusCode::OK) {
        LOG(ERROR) << "database_close failed with " << res;
        return false;
    }
    return true;
}

sharksfin::DatabaseHandle storage_context::handle() const noexcept {
    return db_;
}

std::shared_ptr<transaction_context> const& storage_context::create_transaction() {
    //TODO lock
    return transactions_.emplace_back(std::make_shared<transaction_context>(*this));
}

}

