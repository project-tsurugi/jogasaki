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

#include "database.h"
#include "transaction.h"

namespace jogasaki::kvs {

using namespace sharksfin;

database::~database() noexcept {
    if (handle_) {
        if(auto res = database_dispose(handle_); res != StatusCode::OK) {
            fail();
        }
    }
}

std::unique_ptr<database> database::open(std::map<std::string, std::string> const& options) {
    sharksfin::DatabaseOptions dbopts{};
    for(auto& p : options) {
        dbopts.attribute(p.first, p.second);
    }
    DatabaseHandle handle{};
    if(auto res = sharksfin::database_open(dbopts, &handle); res != sharksfin::StatusCode::OK) {
        LOG(ERROR) << "database_open failed with " << res;
        return {};
    }
    return std::make_unique<database>(handle);
}

bool database::close() {
    if(auto res = sharksfin::database_close(handle_); res != sharksfin::StatusCode::OK) {
        LOG(ERROR) << "database_close failed with " << res;
        return false;
    }
    return true;
}

sharksfin::DatabaseHandle database::handle() const noexcept {
    return handle_;
}

std::unique_ptr<transaction> database::create_transaction() {
    return std::make_unique<transaction>(*this);
}

}

