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
#pragma once

#include <sharksfin/api.h>
#include <takatori/util/exception.h>

namespace jogasaki::api::kvsservice {

/**
 * @brief convert sharksfin::StatusCode to jogasaki::api::kvsservice::status
 * @param code the code of sharksfin::StatusCode
 * @return the value of kvsservice::status corresponding to the code
 */
inline status convert(sharksfin::StatusCode code) {
    switch (code) {
        case sharksfin::StatusCode::OK:
            return status::ok;
        case sharksfin::StatusCode::NOT_FOUND:
            return status::not_found;
        case sharksfin::StatusCode::ALREADY_EXISTS:
            return status::already_exists;
        case sharksfin::StatusCode::USER_ROLLBACK:
            return status::user_rollback;
        case sharksfin::StatusCode::WAITING_FOR_OTHER_TRANSACTION:
            return status::waiting_for_other_transaction;
        case sharksfin::StatusCode::ERR_UNKNOWN:
            return status::err_unknown;
        case sharksfin::StatusCode::ERR_IO_ERROR:
            return status::err_io_error;
        case sharksfin::StatusCode::ERR_INVALID_ARGUMENT:
            return status::err_invalid_argument;
        case sharksfin::StatusCode::ERR_INVALID_STATE:
            return status::err_invalid_state;
        case sharksfin::StatusCode::ERR_UNSUPPORTED:
            return status::err_unsupported;
        case sharksfin::StatusCode::ERR_USER_ERROR:
            return status::err_user_error;
        case sharksfin::StatusCode::ERR_ABORTED:
            return status::err_aborted;
        case sharksfin::StatusCode::ERR_ABORTED_RETRYABLE:
            return status::err_aborted_retryable;
        case sharksfin::StatusCode::ERR_TIME_OUT:
            return status::err_time_out;
        case sharksfin::StatusCode::ERR_NOT_IMPLEMENTED:
            return status::err_not_implemented;
        case sharksfin::StatusCode::ERR_ILLEGAL_OPERATION:
            return status::err_illegal_operation;
        case sharksfin::StatusCode::ERR_CONFLICT_ON_WRITE_PRESERVE:
            return status::err_conflict_on_write_preserve;
        case sharksfin::StatusCode::ERR_WRITE_WITHOUT_WRITE_PRESERVE:
            return status::err_write_without_write_preserve;
        case sharksfin::StatusCode::ERR_INACTIVE_TRANSACTION:
            return status::err_inactive_transaction;
        case sharksfin::StatusCode::ERR_BLOCKED_BY_CONCURRENT_OPERATION:
            return status::err_blocked_by_concurrent_operation;
        case sharksfin::StatusCode::ERR_RESOURCE_LIMIT_REACHED:
            return status::err_resource_limit_reached;
        case sharksfin::StatusCode::ERR_INVALID_KEY_LENGTH:
            return status::err_invalid_key_length;
        default:
            takatori::util::throw_exception(std::logic_error{"unknown code"});
    }
}

/**
 * @brief convert two sharksfin::StatusCode to a jogasaki::api::kvsservice::status
 * @param code1 the code of sharksfin::StatusCode
 * @param code2 the code of sharksfin::StatusCode
 * @return if code1 is not StatusCode::OK, kvsservice::status corresponding to the code1
 * @return otherwise, kvsservice::status corresponding to the code2
 */
inline status convert(sharksfin::StatusCode code1, sharksfin::StatusCode code2) {
    if (status s = convert(code1); s != status::ok) {
        return s;
    }
    return convert(code2);
}

/**
 * @brief convert two jogasaki::api::kvsservice::status to a jogasaki::api::kvsservice::status
 * @param s1 the status of jogasaki::api::kvsservice::status
 * @param s2 the status jogasaki::api::kvsservice::status
 * @return if s1 is not status::ok, returns s1
 * @return otherwise, returns s2
 */
inline status convert(status s1, status s2) {
    if (s1 != status::ok) {
        return s1;
    }
    return s2;
}

/**
 * @brief convert sharksfin::TransactionState::StateKind to jogasaki::api::kvsservice::transaction_state::state_kind
 * @param kind the value of sharksfin::TransactionState::StateKind
 * @return the value of transaction_state::state_kind corresponding to the kind
 */
inline transaction_state::state_kind convert(sharksfin::TransactionState::StateKind kind) {
    switch (kind) {
        case sharksfin::TransactionState::StateKind::UNKNOWN:
            return transaction_state::state_kind::unknown;
        case sharksfin::TransactionState::StateKind::WAITING_START:
            return transaction_state::state_kind::waiting_start;
        case sharksfin::TransactionState::StateKind::STARTED:
            return transaction_state::state_kind::started;
        case sharksfin::TransactionState::StateKind::WAITING_CC_COMMIT:
            return transaction_state::state_kind::waiting_cc_commit;
        case sharksfin::TransactionState::StateKind::ABORTED:
            return transaction_state::state_kind::aborted;
        case sharksfin::TransactionState::StateKind::WAITING_DURABLE:
            return transaction_state::state_kind::waiting_durable;
        case sharksfin::TransactionState::StateKind::DURABLE:
            return transaction_state::state_kind::durable;
        default:
            takatori::util::throw_exception(std::logic_error{"unknown kind"});
    }
}

/**
 * @brief convert jogasaki::api::kvsservice::put_option to sharksfin::PutOperation
 * @param opt the value of jogasaki::api::kvsservice::put_option
 * @return the value of sharksfin::PutOperation corresponding to the opt
 */
inline sharksfin::PutOperation convert(put_option opt) {
    switch (opt) {
        case put_option::create_or_update:
            return sharksfin::PutOperation::CREATE_OR_UPDATE;
        case put_option::create:
            return sharksfin::PutOperation::CREATE;
        case put_option::update:
            return sharksfin::PutOperation::UPDATE;
        default:
            takatori::util::throw_exception(std::logic_error{"unknown put_option"});
    }
}

}