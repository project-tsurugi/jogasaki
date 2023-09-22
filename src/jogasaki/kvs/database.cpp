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

#include <takatori/util/exception.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include "database.h"
#include "transaction.h"
#include "error.h"

namespace jogasaki::kvs {

using namespace sharksfin;
using takatori::util::string_builder;
using takatori::util::throw_exception;

database::~database() noexcept {
    if (handle_ && ! handle_borrowed_) {
        if(auto res = database_dispose(handle_); res != StatusCode::OK) {
            LOG_LP(ERROR) << "database_dispose failed";
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
        LOG_LP(ERROR) << "database_open failed with " << res;
        return {};
    }
    auto ret = std::make_unique<database>(handle);
    ret->handle_borrowed_ = false;
    return ret;
}

bool database::close() {
    if(handle_borrowed_) return true;
    if(auto res = sharksfin::database_close(handle_); res != sharksfin::StatusCode::OK) {
        LOG_LP(ERROR) << "database_close failed with " << res;
        return false;
    }
    return true;
}

sharksfin::DatabaseHandle database::handle() const noexcept {
    return handle_;
}

std::unique_ptr<transaction> database::create_transaction(
    kvs::transaction_option const& options
) {
    std::unique_ptr<transaction> ret{};
    if(auto res = transaction::create_transaction(*this, ret, options); res != status::ok) {
        // left for testing
        return {};
    }
    return ret;
}

std::unique_ptr<storage> database::create_storage(std::string_view name, StorageOptions const& options) {
    sharksfin::StorageHandle stg{};
    if (auto res = sharksfin::storage_create(handle_, sharksfin::Slice(name), options, &stg);
        res == sharksfin::StatusCode::ALREADY_EXISTS) {
        return {};
    } else if (res != sharksfin::StatusCode::OK) { //NOLINT
        throw_exception(std::logic_error{
            string_builder{} << "storage_create failed with error:" << res << string_builder::to_string
        });
    }
    return std::make_unique<storage>(stg);
}

std::unique_ptr<storage> database::get_storage(std::string_view name) {
    sharksfin::StorageHandle stg{};
    if (auto res = sharksfin::storage_get(handle_, sharksfin::Slice(name), &stg);
        res == sharksfin::StatusCode::NOT_FOUND) {
        return {};
    } else if (res != sharksfin::StatusCode::OK) { //NOLINT
        throw_exception(std::logic_error{
            string_builder{} << "storage_get failed with error:" << res << string_builder::to_string
        });
    }
    return std::make_unique<storage>(stg);
}

std::unique_ptr<storage> database::get_or_create_storage(std::string_view name) {
    if(auto p = get_storage(name)) {
        return p;
    }
    if(auto p = create_storage(name)) {
        return p;
    }
    return {};
}

status database::create_sequence(sequence_id& out) {
    sequence_id id{};
    if (auto res = sharksfin::sequence_create(handle_, &id); res != sharksfin::StatusCode::OK) {
        return resolve(res);
    }
    out = id;
    return status::ok;
}

status database::update_sequence(transaction& tx, sequence_id id, sequence_version version, sequence_value value) noexcept {
    if (auto res = sharksfin::sequence_put(
            tx.handle(),
            id,
            version,
            value
        ); res != sharksfin::StatusCode::OK) {
        if (res == sharksfin::StatusCode::NOT_FOUND) {
            return status::err_not_found;
        }
        return resolve(res);
    }
    return status::ok;
}

status database::read_sequence(sequence_id id, sequence_versioned_value& out) {
    sequence_versioned_value ret{};
    if (auto res = sharksfin::sequence_get(
            handle_,
            id,
            &ret.version_,
            &ret.value_
        ); res != sharksfin::StatusCode::OK) {
        if (res == sharksfin::StatusCode::NOT_FOUND) {
            return status::err_not_found;
        }
        return resolve(res);
    }
    out = ret;
    return status::ok;
}

status database::delete_sequence(sequence_id id) {
    if (auto res = sharksfin::sequence_delete(
            handle_,
            id
        ); res != sharksfin::StatusCode::OK) {
        if (res == sharksfin::StatusCode::NOT_FOUND) {
            return status::err_not_found;
        }
        return resolve(res);
    }
    return status::ok;
}

database::database(DatabaseHandle handle) : handle_(handle) {}

status database::list_storages(std::vector<std::string>& out) const noexcept {
    return resolve(sharksfin::storage_list(handle_, out));
}

status database::register_durability_callback(::sharksfin::durability_callback_type cb) {
    ::sharksfin::database_register_durability_callback(handle_, std::move(cb));
    return status::ok;
}

database::database() = default;

}

