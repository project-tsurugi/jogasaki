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


#include <sharksfin/api.h>
#include <hayatsuki/collector/shirakami/collector.h>
#include <hayatsuki/log_record.h>

#include <jogasaki/configuration.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::logship {

namespace details {
class buffer;
}

using LogRecord = ::sharksfin::LogRecord;

class log_event_listener {
public:
    log_event_listener();

    ~log_event_listener();

    bool init(configuration& cfg);

    bool operator()(std::size_t worker, LogRecord* begin, LogRecord* end);

    bool deinit();
private:
    std::unique_ptr<::hayatsuki::ShirakamiCollector> collector_{std::make_unique<::hayatsuki::ShirakamiCollector>()};
    std::unordered_map<sharksfin::LogRecord::storage_id_type, meta::record_meta> meta_{};
    std::vector<std::unique_ptr<details::buffer>> buffers_{};
};

std::unique_ptr<log_event_listener> create_log_event_listener(configuration& cfg);
}


