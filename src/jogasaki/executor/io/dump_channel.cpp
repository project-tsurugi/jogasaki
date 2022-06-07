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
#include "dump_channel.h"

#include <jogasaki/executor/io/record_channel.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/io/data_channel_writer.h>
#include <jogasaki/executor/io/dump_channel_writer.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::executor::io {

dump_channel::dump_channel(
    maybe_shared_ptr<record_channel> channel,
    std::string_view directory,
    dump_cfg cfg
) noexcept:
    channel_(std::move(channel)),
    file_name_record_meta_(
        std::make_shared<meta::external_record_meta>(
            std::make_shared<meta::record_meta>(
                std::vector<meta::field_type>{
                    meta::field_type(meta::field_enum_tag<meta::field_type_kind::character>),
                },
                boost::dynamic_bitset<std::uint64_t>{1}.flip()
            ),
            std::vector<std::optional<std::string>>{"file_name"}
        )
    ),
    directory_(directory),
    cfg_(cfg)
{
    auto now = std::chrono::system_clock::now();
    auto secs_since_epoch = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    prefix_ = std::string("d") + std::to_string(secs_since_epoch);
}

status dump_channel::acquire(std::shared_ptr<record_writer>& wrt) {
    std::shared_ptr<record_writer> w{};
    channel_->acquire(w);
    auto wid = writer_id_src_++;
    wrt = std::make_shared<dump_channel_writer>(*this, w, wid, cfg_);
    return status::ok;
}

record_channel& dump_channel::channel() {
    return *channel_;
}

status dump_channel::meta(maybe_shared_ptr<meta::external_record_meta> m) {
    meta_ = std::move(m);
    channel_->meta(file_name_record_meta_);
    return status::ok;
}

std::string_view dump_channel::directory() const noexcept {
    return directory_;
}

maybe_shared_ptr<meta::external_record_meta> const& dump_channel::meta() const noexcept {
    return meta_;
}

maybe_shared_ptr<meta::external_record_meta> const& dump_channel::file_name_record_meta() const noexcept {
    return file_name_record_meta_;
}

std::string_view dump_channel::prefix() const noexcept {
    return prefix_;
}

}
