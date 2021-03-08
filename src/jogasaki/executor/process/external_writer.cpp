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
#include "external_writer.h"

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/executor/record_writer.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor::process {

using takatori::util::maybe_shared_ptr;

bool external_writer::write(accessor::record_ref rec) {
    store_->append(rec);
    return false;
}

void external_writer::flush() {
    // no-op
}

void external_writer::release() {
    store_ = nullptr;
}

external_writer::external_writer(
    data::iterable_record_store &store,
    maybe_shared_ptr<meta::record_meta> meta
) :
    store_(std::addressof(store)),
    meta_(std::move(meta))
{}

}
