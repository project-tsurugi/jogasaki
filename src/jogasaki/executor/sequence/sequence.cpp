/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "sequence.h"

#include <atomic>
#include <utility>

#include <jogasaki/common_types.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/sequence/info.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::executor::sequence {

sequence::sequence(
    class info const& info,
    manager& parent,
    sequence_version version,
    sequence_value value
) :
    info_(std::addressof(info)),
    parent_(std::addressof(parent)),
    body_({version, value})
{}

sequence_versioned_value sequence::get() const noexcept {
    return body_.load();
}

either<sequence_error, sequence_value> sequence::next(kvs::transaction& tx) {
    parent_->mark_sequence_used_by(tx, *this);
    aligned_sequence_versioned_value cur{};
    aligned_sequence_versioned_value next{};
    do {
        cur = body_.load();
        if(cur.version_ == initial_sequence_version) {
            // the first version is the special case and use initial value
            next = {cur.version_ + 1, info_->initial_value()};
            continue;
        }
        sequence_value val{};
        // the current value can be out of [min, max] just after reset(), so the addition below can
        // overflow. Note the overflow direction is same as the sign of the increment.
        sequence_value cand{};
        bool overflow = __builtin_add_overflow(cur.value_, info_->increment(), std::addressof(cand));
        if (info_->increment() > 0 && (overflow || cand > info_->maximum_value())) {
            if(info_->cycle()) {
                val = info_->minimum_value();
            } else {
                return sequence_error::out_of_upper_bound;
            }
        } else if (info_->increment() < 0 && (overflow || cand < info_->minimum_value())) {
            if(info_->cycle()) {
                val = info_->maximum_value();
            } else {
                return sequence_error::out_of_lower_bound;
            }
        } else {
            val = cand;
        }
        next = {cur.version_ + 1, val};
    } while(! body_.compare_exchange_strong(cur, next));
    return next.value_;
}

bool sequence::can_reset() const noexcept {
    sequence_value val{};
    return ! __builtin_sub_overflow(info_->initial_value(), info_->increment(), std::addressof(val));
}

void sequence::reset(kvs::transaction& tx) {
    // the caller must verify the sequence is resettable in advance
    assert_with_exception(can_reset());
    parent_->mark_sequence_used_by(tx, *this);
    aligned_sequence_versioned_value cur{};
    aligned_sequence_versioned_value next{};
    do {
        cur = body_.load();
        if(cur.version_ == initial_sequence_version) {
            // the sequence is not used yet and next() already returns the initial value
            return;
        }
        // set the value so that the following next() returns the initial value,
        // while keeping the version increasing so that the update can be made durable
        sequence_value val{};
        if(__builtin_sub_overflow(info_->initial_value(), info_->increment(), std::addressof(val))) {
            // should not happen because can_reset() is checked above
            fail_with_exception();
        }
        next = {cur.version_ + 1, val};
    } while(! body_.compare_exchange_strong(cur, next));
}

class info const& sequence::info() const noexcept {
    return *info_;
}

}  // namespace jogasaki::executor::sequence
