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
#include <msgpack.hpp>

#include <yugawara/storage/configurable_provider.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/fail.h>

#ifdef LOGSHIP
#include <hayatsuki/collector/shirakami/collector.h>
#include <hayatsuki/log_record.h>
#else
#include "hayatsuki_mock.h"
#endif

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/index/index_accessor.h>
#include <jogasaki/index/field_factory.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/utils/result_serialization.h>

namespace jogasaki::logship {

using takatori::util::fail;

namespace details {


class buffer {
public:
    using memory_resource = memory::lifo_paged_memory_resource;

    std::vector<hayatsuki::log_record>& records() {
        return output_;
    }

    void clear() {
        output_.clear();
        resource_->deallocate_after(memory_resource::initial_checkpoint);
        data_.resize(0);
        buf_.clear();
        offset_ = 0;
    }

    data::aligned_buffer& data() {
        return data_;
    }

    memory_resource* resource() noexcept {
        return resource_.get();
    }

    msgpack::sbuffer& msgbuffer() noexcept {
        return buf_;
    }

    std::size_t& offset() noexcept {
        return offset_;
    }
private:
    std::vector<hayatsuki::log_record> output_{};
    data::aligned_buffer data_{};
    std::unique_ptr<memory_resource> resource_{std::make_unique<memory_resource>(&global::page_pool())};
    msgpack::sbuffer buf_{0};
    std::size_t offset_{};
};

}

log_event_listener::log_event_listener() = default;

log_event_listener::~log_event_listener() = default;

bool log_event_listener::init(jogasaki::configuration& cfg) {
    auto sz = cfg.max_logging_parallelism();
    if(auto rc = collector_->init(sz); rc != 0) {
        VLOG_LP(log_error) << collector_->get_error_message(rc);
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
        case s::UPSERT: return h::UPSERT;
    }
    fail();
}

storage_data const& log_event_listener::find_storage(LogRecord::storage_id_type storage_id) {
    while(true) {
        {
            decltype(index_mappers_)::accessor acc{};
            if (index_mappers_.find(acc, storage_id)) {
                return acc->second;
            }
        }
        std::shared_ptr<yugawara::storage::index const> found{};
        provider_->each_index([&](std::string_view id, std::shared_ptr<yugawara::storage::index const> const& entry) {
            (void) id;
            if (entry->definition_id() && entry->definition_id() == storage_id) {
                found = entry;
            }
        });
        auto mapper = std::make_shared<index::mapper>(
            index::index_fields(*found, true),
            index::index_fields(*found, false)
        );
        auto key_meta = index::create_meta(*found, true);
        auto value_meta = index::create_meta(*found, false);
        {
            decltype(index_mappers_)::accessor acc{};
            if (index_mappers_.insert(acc, storage_id)) {
                acc->second = {std::move(mapper), std::move(key_meta), std::move(value_meta)};
                return acc->second;
            }
        }
        // insert failed, so re-try from fetching
    }
}

bool log_event_listener::convert(
    bool key,
    std::string_view data,
    LogRecord::storage_id_type id,
    details::buffer& buf,
    std::string_view& out
) {
    auto& [mapper, key_meta, value_meta] = find_storage(id);
    kvs::readable_stream stream{data.data(), data.size()};
    auto& meta = key ? key_meta : value_meta;
    auto sz = meta->record_size();
    buf.data().resize(sz);
    accessor::record_ref rec{buf.data().data(), sz};
    if(! mapper->read(key, stream, rec, buf.resource())) {
        return false;
    }
    if(! utils::write_msg(rec, buf.msgbuffer(), meta.get())) {
        return false;
    }
    auto len = buf.msgbuffer().size();
    auto p = static_cast<char*>(buf.resource()->allocate(len));
    std::memcpy(p, buf.msgbuffer().data(), len);
    out = {p, len};
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
        if(! convert(true, it->key_, it->storage_id_, *buf, k)) {
            VLOG_LP(log_error) << "error conversion: " << it->key_;
            return false;
        }
        std::string_view v{};
        if(! convert(false, it->value_, it->storage_id_, *buf, v)) {
            VLOG_LP(log_error) << "error conversion: " << it->value_;
            return false;
        }
        buf->records().emplace_back(
            from(it->operation_),
            k,
            v,
            it->major_version_,
            it->minor_version_,
            it->storage_id_
        );
        ++it; //NOLINT
    }
    if(auto rc = collector_->write_message(worker, buf->records()); rc != 0) {
        VLOG_LP(log_error) << collector_->get_error_message(rc);
        return false;
    }
    return true;
}

bool log_event_listener::deinit() {
    if(auto rc = collector_->finish(); rc != 0) {
        VLOG_LP(log_error) << collector_->get_error_message(rc);
        return false;
    }
    return true;
}

log_event_listener::log_event_listener(std::shared_ptr<yugawara::storage::configurable_provider> provider) :
    collector_(std::make_unique<::hayatsuki::ShirakamiCollector>()),
    provider_(std::move(provider))
{}

std::unique_ptr<log_event_listener> create_log_event_listener(configuration& cfg, std::shared_ptr<yugawara::storage::configurable_provider> provider) {
    auto ret = std::make_unique<log_event_listener>(std::move(provider));
    if(auto rc = ret->init(cfg); ! rc) {
        VLOG_LP(log_error) << "creating log_event_listener failed.";
        return {};
    }
    return ret;
}
}


