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
#include "data_writer.h"

#include <memory>
#include <ostream>
#include <utility>
#include <glog/logging.h>

#include <tateyama/api/server/writer.h>
#include <tateyama/status.h>

#include <jogasaki/logging_helper.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/trace_log.h>

namespace jogasaki::api::impl {

data_writer::data_writer(std::shared_ptr<tateyama::api::server::writer> origin) :
    origin_(std::move(origin))
{}

status data_writer::write(char const* data, std::size_t length) {
    log_entry << binstring(data, length);
    status ret = status::ok;
    if (auto rc = origin_->write(data, length); rc != tateyama::status::ok) {
        // writer overflow may be specific to some request, but it's possibly
        // a system-wide issue (e.g. bad configuration), so use LOG instead of VLOG to notify DB admin
        LOG_LP(ERROR) << "failed to write data possibly due to writer buffer overflow";
        ret = status::err_io_error;
    }
    log_exit << ret;
    return ret;
}

status data_writer::commit() {
    log_entry;
    status ret = status::ok;
    if (auto rc = origin_->commit(); rc != tateyama::status::ok) {
        // using LOG for same reason as in data_writer::write failure
        LOG_LP(ERROR) << "failed to commit writer data";
        ret = status::err_io_error;
    }
    log_exit << ret;
    return ret;
}

std::shared_ptr<tateyama::api::server::writer> const& data_writer::origin() const noexcept {
    return origin_;
}

}  // namespace jogasaki::api::impl
