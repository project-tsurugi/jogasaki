/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <type_traits>
#include <utility>

#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/executor/io/record_channel_stats.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/meta/external_record_meta.h>

namespace jogasaki::api::impl {

using namespace jogasaki::executor;

result_store_channel_writer::result_store_channel_writer(result_store_channel& parent, std::size_t index) noexcept:
    parent_(std::addressof(parent)),
    index_(index)
{}

bool result_store_channel_writer::write(accessor::record_ref rec) {
    auto& st = parent_->store().partition(index_);
    st.append(rec);
    return true;
}

std::size_t result_store_channel_writer::index() const noexcept {
    return index_;
}

void result_store_channel_writer::release() {
    parent_->statistics().add_total_record(write_count());
}

void result_store_channel_writer::flush() {
    // no-op
}

std::size_t result_store_channel_writer::write_count() const noexcept {
    auto& st = parent_->store().partition(index_);
    return st.count();
}

result_store_channel::result_store_channel(maybe_shared_ptr<data::result_store> store) noexcept:
    store_(std::move(store))
{}

status result_store_channel::acquire(std::shared_ptr<io::record_writer>& wrt) {
    auto idx = store_->add_partition();
    wrt = std::make_shared<result_store_channel_writer>(*this, idx);
    return status::ok;
}

data::result_store& result_store_channel::store() {
    return *store_;
}

status result_store_channel::meta(maybe_shared_ptr<meta::external_record_meta> m) {
    store_->initialize(m->origin());
    return status::ok;
}

executor::io::record_channel_stats& result_store_channel::statistics() {
    return stats_;
}

}
