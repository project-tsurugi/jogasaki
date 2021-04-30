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

std::unique_ptr<transaction> database::create_transaction(bool readonly) {
    return std::make_unique<transaction>(*this, readonly);
}

std::unique_ptr<storage> database::create_storage(std::string_view name) {
    sharksfin::StorageHandle stg{};
    if (auto res = sharksfin::storage_create(handle_, sharksfin::Slice(name), &stg);
        res == sharksfin::StatusCode::ALREADY_EXISTS) {
        return {};
    } else if (res != sharksfin::StatusCode::OK) { //NOLINT
        fail();
    }
    return std::make_unique<storage>(stg);
}

std::unique_ptr<storage> database::get_storage(std::string_view name) {
    sharksfin::StorageHandle stg{};
    if (auto res = sharksfin::storage_get(handle_, sharksfin::Slice(name), &stg);
        res == sharksfin::StatusCode::NOT_FOUND) {
        return {};
    } else if (res != sharksfin::StatusCode::OK) { //NOLINT
        fail();
    }
    return std::make_unique<storage>(stg);
}

sequence_id database::create_sequence() noexcept {
    sequence_id id{};
    if (auto res = sharksfin::sequence_create(handle_, &id); res != sharksfin::StatusCode::OK) {
        fail();
    }
    return id;
}

bool database::update_sequence(transaction& tx, sequence_id id, sequence_version version, sequence_value value) noexcept {
    if (auto res = sharksfin::sequence_put(
            tx.handle(),
            id,
            version,
            value
        ); res != sharksfin::StatusCode::OK) {
        fail();
    }
    return true;
}

sequence_versioned_value database::read_sequence(sequence_id id) noexcept {
    sequence_versioned_value ret{};
    if (auto res = sharksfin::sequence_get(
            handle_,
            id,
            &ret.version_,
            &ret.value_
        ); res != sharksfin::StatusCode::OK) {
        if (res == sharksfin::StatusCode::NOT_FOUND) {
            return {version_invalid, 0};
        }
        fail();
    }
    return ret;
}

bool database::delete_sequence(sequence_id id) noexcept {
    if (auto res = sharksfin::sequence_delete(
            handle_,
            id
        ); res != sharksfin::StatusCode::OK) {
        fail();
    }
    return true;
}

}

