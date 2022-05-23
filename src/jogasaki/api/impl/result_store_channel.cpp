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
#include "result_store_channel.h"

#include <jogasaki/api/data_channel.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::api::impl {

result_store_channel_writer::result_store_channel_writer(result_store_channel& parent, std::size_t index) noexcept:
    parent_(std::addressof(parent)),
    index_(index)
{}

bool result_store_channel_writer::write(accessor::record_ref rec) {
    auto& st = parent_->store().store(index_);
    st.append(rec);
    return false;
}

std::size_t result_store_channel_writer::index() const noexcept {
    return index_;
}

result_store_channel::result_store_channel(maybe_shared_ptr<data::result_store> store) noexcept:
    store_(std::move(store))
{}

status result_store_channel::acquire(std::shared_ptr<executor::record_writer>& wrt) {
    auto idx = store_->add_store();
    wrt = std::make_shared<result_store_channel_writer>(*this, idx);
    return status::ok;
}

status result_store_channel::release(executor::record_writer& wrt) {
    auto& w = static_cast<result_store_channel_writer&>(wrt);
    store_->clear_store(w.index());
    return status::ok;
}
}
