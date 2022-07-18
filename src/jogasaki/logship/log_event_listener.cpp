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
#include "log_event_listener.h"

#include <glog/logging.h>
#include <jogasaki/logging.h>

#include <takatori/util/fail.h>

namespace jogasaki::logship {

using takatori::util::fail;

bool log_event_listener::init(jogasaki::configuration& cfg) {
    auto sz = cfg.max_logging_parallelism();
    if(auto rc = collector_->init(sz); rc != 0) {
        VLOG(log_error) << collector_->get_error_message(rc);
        return false;
    }
    buffers_.resize(sz);
    return true;
}

::hayatsuki::LogOperation from(::sharksfin::LogOperation op) {
    using s = ::sharksfin::LogOperation;
    using h = ::hayatsuki::LogOperation;
    switch(op) {
        case s::UNKNOWN: return h::UNKNOWN;
        case s::INSERT: return h::INSERT;
        case s::UPDATE: return h::UPDATE;
        case s::DELETE: return h::DELETE;
    }
    fail();
}

bool convert(bool key, std::string_view data, details::buffer& buf, std::string_view& out) {
    (void)key;
    (void)buf;
    out = data;
    return true;
}

bool log_event_listener::operator()(std::size_t worker, LogRecord* begin, LogRecord* end) {
    BOOST_ASSERT(worker < buffers_.size());  //NOLINT
    auto& buf = buffers_[worker];
    if(! buf) {
        buf = std::make_unique<details::buffer>();
    }
    buf->clear();
    LogRecord* it = begin;
    while(it != end) {
        std::string_view k{};
        if(! convert(true, it->key_, *buf, k)) {
            VLOG(log_error) << "error conversion: " << it->key_;
            return false;
        }
        std::string_view v{};
        if(! convert(false, it->value_, *buf, v)) {
            VLOG(log_error) << "error conversion: " << it->value_;
            return false;
        }
        buf->records().emplace_back(
            from(it->operation_),
            k, // TODO
            v, // TODO
            it->major_version_,
            it->minor_version_,
            it->storage_id_
        );
        ++it;
    }
    if(auto rc = collector_->write_message(worker, buf->records()); rc != 0) {
        VLOG(log_error) << collector_->get_error_message(rc);
        return false;
    }
    return true;
}

bool log_event_listener::deinit() {
    if(auto rc = collector_->finish(); rc != 0) {
        VLOG(log_error) << collector_->get_error_message(rc);
        return false;
    }
    return true;
}

}


