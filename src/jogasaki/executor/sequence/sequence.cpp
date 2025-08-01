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
        if (info_->increment() > 0 && info_->maximum_value() - cur.value_ < info_->increment()) {
            if(info_->cycle()) {
                val = info_->minimum_value();
            } else {
                return sequence_error::out_of_upper_bound;
            }
        } else if (info_->increment() < 0 && cur.value_ - info_->minimum_value() < -info_->increment()) {
            if(info_->cycle()) {
                val = info_->maximum_value();
            } else {
                return sequence_error::out_of_lower_bound;
            }
        } else {
            val = cur.value_ + info_->increment();
        }
        next = {cur.version_ + 1, val};
    } while(! body_.compare_exchange_strong(cur, next));
    return next.value_;
}

class info const& sequence::info() const noexcept {
    return *info_;
}

}  // namespace jogasaki::executor::sequence
