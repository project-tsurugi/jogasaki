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
#include "transaction_state.h"

#include <glog/logging.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>

namespace jogasaki {

namespace details {
bool transition_allowed(transaction_state_kind cur, transaction_state_kind dest) {
    // check if transition is allowed or not.
    // it is not intended to make complete list of allowed/disallowed transitions,
    // but we prohibit clearly invalid transitions (e.g. aborted -> going_to_abort)
    // that we might have overlooked in the threaded flow of control.
    using kind = transaction_state_kind;
    if (dest == cur) {
        // actually this is allowed, but no need to change, so return false
        return false;
    }
    if (dest == kind::unknown) {
        // unknown state is allowed from any state (e.g. cancel request makes tx status unknown)
        return true;
    }
    if (dest == kind::init || dest == kind::undefined) {
        // undefined -> init is only allowed
        return cur == kind::undefined;
    }
    switch (cur) {
    case kind::undefined:
        // only to init is allowed covered above
            return false;
    case kind::init:
        // any state is allowed
            return true;
    case kind::active:
        // any state allowed except init (which is covered above)
            return true;
    case kind::going_to_commit:
        // tx termination is in progress
            return dest == kind::cc_committing || dest == kind::committed_available || dest == kind::committed_stored || dest == kind::aborted;
    case kind::going_to_abort:
        // only aborted is allowed
            return dest == kind::aborted;
    case kind::cc_committing:
        return dest == kind::committed_available || dest == kind::committed_stored || dest == kind::aborted;
    case kind::aborted:
        return false;
    case kind::committed_available:
        return dest == kind::committed_stored;
    case kind::committed_stored:
        return false;
    case kind::unknown:
        return false;
    }
    std::abort();
}

}  // namespace details

transaction_state_kind transaction_state::kind() const noexcept {
    return state_.load();
}

void transaction_state::set(transaction_state_kind desired) noexcept {
    auto cur = state_.load();
    do {
        if (! details::transition_allowed(cur, desired)) {
            return;
        }
    } while (! state_.compare_exchange_weak(cur, desired));
    VLOG_LP(log_trace) << "state:" << cur << " -> " << desired;
}

bool transaction_state::set_if(transaction_state_kind expected, transaction_state_kind desired) noexcept {
    auto ret = state_.compare_exchange_strong(expected, desired);
    if (ret) {
        VLOG_LP(log_trace) << "state:" << expected << " -> " << desired;
    }
    return ret;
}

} // namespace jogasaki
