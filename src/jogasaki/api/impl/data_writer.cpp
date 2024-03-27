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
    if (auto rc = origin_->write(data, length); rc != tateyama::status::ok) {
        fail_with_exception();
    }
    log_exit;
    return status::ok;
}

status data_writer::commit() {
    log_entry;
    if (auto rc = origin_->commit(); rc != tateyama::status::ok) {
        fail_with_exception();
    }
    log_exit;
    return status::ok;
}

std::shared_ptr<tateyama::api::server::writer> const& data_writer::origin() const noexcept {
    return origin_;
}
}

