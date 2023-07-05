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
#include <boost/filesystem.hpp>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/io/dump_channel.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor::io {

using takatori::util::maybe_shared_ptr;

dump_channel_writer::dump_channel_writer(
    dump_channel& parent,
    maybe_shared_ptr<record_writer> writer,
    std::size_t writer_index,
    dump_cfg cfg
) :
    parent_(std::addressof(parent)),
    writer_(std::move(writer)),
    writer_index_(writer_index),
    cfg_(cfg)
{}

void dump_channel_writer::release() {
    if (parquet_writer_) {
        close_parquet_writer();
    }
    writer_->release();
}

std::string dump_channel_writer::create_file_name(std::string_view prefix) const {
    return
        std::string{prefix} + "_" + std::to_string(writer_index_) + "_" +
        std::to_string(current_sequence_number_) + ".parquet";
}

bool dump_channel_writer::write(accessor::record_ref rec) {
    if (! parquet_writer_) {
        auto fn = create_file_name(parent_->prefix());
        boost::filesystem::path p(std::string{parent_->directory()});
        p = p / fn;
        parquet_writer_ = file::parquet_writer::open(parent_->meta(), p.string());
        if (! parquet_writer_) {
            VLOG_LP(log_error) << "parquet file creation failed on path " << p.string();
            return false;
        }
    }
    if(auto res = parquet_writer_->write(rec); ! res) {
        return false;
    }
    if(cfg_.max_records_per_file_ != dump_cfg::undefined && parquet_writer_->write_count() >= cfg_.max_records_per_file_) {
        flush();
    }
    return true;
}

void dump_channel_writer::write_file_path(std::string_view path) {
    auto& meta = *parent_->file_name_record_meta();
    auto sz = meta.record_size();
    std::vector<char> buf(sz, 0);
    accessor::record_ref ref{std::addressof(buf[0]), sz};
    ref.set_value(meta.value_offset(0), accessor::text(path));
    ref.set_null(meta.nullity_offset(0), false);
    writer_->write(ref);
    writer_->flush();
    parent_->add_output_file(path);
}

void dump_channel_writer::close_parquet_writer() {
    parquet_writer_->close();
    write_file_path(parquet_writer_->path());
    parquet_writer_.reset();
    ++current_sequence_number_;
}

void dump_channel_writer::flush() {
    if (parquet_writer_) {
        close_parquet_writer();
    }
}

}
