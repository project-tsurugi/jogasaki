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

#include <tbb/concurrent_hash_map.h>

#include <yugawara/storage/configurable_provider.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <sharksfin/api.h>
#include <hayatsuki/collector/shirakami/collector.h>
#include <hayatsuki/log_record.h>

#include <jogasaki/configuration.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/index/index_accessor.h>

namespace jogasaki::logship {

namespace details {
class buffer;
}

using LogRecord = ::sharksfin::LogRecord;
using takatori::util::maybe_shared_ptr;

struct storage_data {
    std::shared_ptr<index::mapper> mapper_{};
    maybe_shared_ptr<meta::record_meta> key_meta_{};
    maybe_shared_ptr<meta::record_meta> value_meta_{};
};

class log_event_listener {
public:
    log_event_listener();

    explicit log_event_listener(std::shared_ptr<yugawara::storage::configurable_provider> provider);

    ~log_event_listener();

    bool init(configuration& cfg);

    bool operator()(std::size_t worker, LogRecord* begin, LogRecord* end);

    bool deinit();

private:
    std::unique_ptr<::hayatsuki::ShirakamiCollector> collector_{std::make_unique<::hayatsuki::ShirakamiCollector>()};
    tbb::concurrent_hash_map<sharksfin::LogRecord::storage_id_type, storage_data> index_mappers_{};
    std::vector<std::unique_ptr<details::buffer>> buffers_{};
    std::shared_ptr<yugawara::storage::configurable_provider> provider_{};

    storage_data const& find_storage(LogRecord::storage_id_type id);
    bool convert(bool key, std::string_view data, LogRecord::storage_id_type id, details::buffer& buf, std::string_view& out);
};

std::unique_ptr<log_event_listener> create_log_event_listener(configuration& cfg, std::shared_ptr<yugawara::storage::configurable_provider> provider);
}


