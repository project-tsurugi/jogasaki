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

#include <jogasaki/logship/log_event_listener.h>
#include "database.h"
#include "transaction.h"

namespace jogasaki::kvs {

using namespace sharksfin;

database::~database() noexcept {
    if (handle_ && ! handle_borrowed_) {
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
    auto ret = std::make_unique<database>(handle);
    ret->handle_borrowed_ = false;
    return ret;
}

bool database::close() {
    if(handle_borrowed_) return true;
    if(auto res = sharksfin::database_close(handle_); res != sharksfin::StatusCode::OK) {
        LOG(ERROR) << "database_close failed with " << res;
        return false;
    }
    return true;
}

sharksfin::DatabaseHandle database::handle() const noexcept {
    return handle_;
}

std::unique_ptr<transaction> database::create_transaction(
    bool readonly,
    bool is_long,
    std::vector<std::string> const& write_preserve
) {
    kvs::transaction_option opts{
        readonly ? transaction_option::transaction_type::read_only:
            (is_long ? transaction_option::transaction_type::ltx : transaction_option::transaction_type::occ),
            write_preserve
    };
    return std::make_unique<transaction>(*this, opts);
}

std::unique_ptr<storage> database::create_storage(std::string_view name, std::uint64_t storage_id) {
    sharksfin::StorageHandle stg{};
    StorageOptions options{};
    if(storage_id != undefined_storage_id) {
        options.storage_id(storage_id);
    }
    if (auto res = sharksfin::storage_create(handle_, sharksfin::Slice(name), options, &stg);
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

std::unique_ptr<storage> database::get_or_create_storage(std::string_view name) {
    if(auto p = get_storage(name)) {
        return p;
    }
    if(auto p = create_storage(name)) {
        return p;
    }
    return {};
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

void database::log_event_listener(std::unique_ptr<logship::log_event_listener> listener) {
    listener_ = std::move(listener);
    ::sharksfin::database_set_logging_callback(handle_, [p = listener_.get()](std::size_t worker, LogRecord* begin, LogRecord* end) {
        (*p)(worker, begin, end);
    });
}

logship::log_event_listener* database::log_event_listener() noexcept {
    return listener_.get();
}

database::database(DatabaseHandle handle) : handle_(handle) {}

database::database() = default;

}

