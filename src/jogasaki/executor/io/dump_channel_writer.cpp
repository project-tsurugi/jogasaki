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
#include "dump_channel_writer.h"

#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/io/dump_channel.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/accessor/record_printer.h>

namespace jogasaki::executor {

using takatori::util::maybe_shared_ptr;

dump_channel_writer::dump_channel_writer(dump_channel& parent, maybe_shared_ptr<executor::record_writer> writer) :
    parent_(std::addressof(parent)),
    writer_(std::move(writer))
{}

void dump_channel_writer::release() {
    writer_->release();
}

bool dump_channel_writer::write(accessor::record_ref rec) {
    auto& meta = *parent_->meta();
    LOG(INFO) << rec << *meta.origin();
    std::size_t cnt = 0;
    std::string out0{std::string{parent_->directory()}+"/dump_output_file_"+std::to_string(++cnt)};

    char buf[1024] = {0};
    accessor::record_ref ref{buf, meta.record_size()};
    ref.set_value(meta.value_offset(0), accessor::text(out0));
    ref.set_null(meta.nullity_offset(0), false);
    writer_->write(ref);
    writer_->write(ref);
    writer_->flush();
    return false;
}

}
