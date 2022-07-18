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
#pragma once

#include <takatori/util/fail.h>

#include <sharksfin/api.h>
#include <hayatsuki/collector/shirakami/collector.h>
#include <hayatsuki/log_record.h>

#include <jogasaki/meta/record_meta.h>

namespace jogasaki::logship {

using LogRecord = ::sharksfin::LogRecord;
using takatori::util::fail;

class log_event_listener {
public:
    log_event_listener() = default;

    void init(configuration& cfg) {
        collector_->init(cfg.max_logging_parallelism());
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


    void operator()(std::size_t worker, LogRecord* begin, LogRecord* end) {
        records_.clear();
        LogRecord* it = begin;
        while(it != end) {
            records_.emplace_back(
                from(it->operation_),
                it->key_, // TODO
                it->value_, // TODO
                it->major_version_,
                it->minor_version_,
                it->storage_id_
            );
            ++it;
        }
        collector_->write_message(worker, records_);
    }

private:
    std::unique_ptr<::hayatsuki::ShirakamiCollector> collector_{};
    std::vector<hayatsuki::log_record> records_{};
    std::unordered_map<sharksfin::LogRecord::storage_id_type, meta::record_meta> meta_{};
};

}


