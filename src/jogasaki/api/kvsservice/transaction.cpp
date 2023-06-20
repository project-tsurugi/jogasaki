/*
 * Copyright 2018-2023 tsurugi project.
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

#include <jogasaki/api/kvsservice/transaction.h>

namespace jogasaki::api::kvsservice {

transaction::transaction(sharksfin::TransactionControlHandle handle) : ctrl_handle_(handle){
    // FIXME call sharksfin: transaction_borrow_handle
    tx_handle_ = nullptr;
    system_id_ = (std::uint64_t)(this); // NOLINT
}

std::uint64_t transaction::system_id() const noexcept {
    return system_id_;
}

transaction_state transaction::state() const {
    // FIXME call sharksfin: transaction_check
    transaction_state s {transaction_state::state_kind::started};
    return s;
}

std::mutex &transaction::transaction_mutex() {
    return mtx_tx_;
}

status transaction::commit() {
    // FIXME call sharksfin: transaction_commit
    return status::ok;
}

status transaction::abort() {
    // FIXME call sharksfin: transaction_abort
    return status::ok;
}

status transaction::put(std::string_view, tateyama::proto::kvs::data::Record const &,
                        put_option) {
    // FIXME call sharksfin
    return status::ok;
}

status transaction::get(std::string_view, tateyama::proto::kvs::data::Record const &primary_key,
                        tateyama::proto::kvs::data::Record &record) {
    // FIXME call sharksfin
    for (auto i = 0; i < primary_key.names_size(); i++) {
        std::string name = primary_key.names(i);
        record.mutable_names()->Add(std::move(name));
        auto value = primary_key.values(i);
        record.mutable_values()->AddAllocated(&value);
    }
    return status::ok;
}

status transaction::remove(std::string_view, tateyama::proto::kvs::data::Record const &,
                        remove_option) {
    // FIXME call sharksfin
    return status::ok;
}

}

