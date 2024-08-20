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
#include "dump_channel_writer.h"

#include <algorithm>
#include <ostream>
#include <type_traits>
#include <utility>
#include <vector>
#include <boost/filesystem/path.hpp>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/file/arrow_writer.h>
#include <jogasaki/executor/file/file_writer.h>
#include <jogasaki/executor/file/parquet_writer.h>
#include <jogasaki/executor/io/dump_channel.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/external_record_meta.h>

namespace jogasaki::executor::io {

using takatori::util::maybe_shared_ptr;

dump_channel_writer::dump_channel_writer(
    dump_channel& parent,
    maybe_shared_ptr<record_writer> writer,
    std::size_t writer_index,
    dump_config cfg
) :
    parent_(std::addressof(parent)),
    writer_(std::move(writer)),
    writer_index_(writer_index),
    cfg_(cfg)
{}

void dump_channel_writer::release() {
    if (file_writer_) {
        close_file_writer();
    }
    writer_->release();
}

std::string dump_channel_writer::create_file_name(std::string_view prefix, dump_config const& cfg) const {
    return
        std::string{prefix} + "_" + std::to_string(writer_index_) + "_" +
        std::to_string(current_sequence_number_) +
        (cfg.file_format_ == dump_file_format_kind::arrow ? ".arrow" : ".parquet");
}

bool dump_channel_writer::write(accessor::record_ref rec) {
    if (! file_writer_) {
        auto fn = create_file_name(parent_->prefix(), cfg_);
        boost::filesystem::path p(std::string{parent_->directory()});
        p = p / fn;
        if(cfg_.file_format_ == dump_file_format_kind::arrow) {
            file::arrow_writer_option opt{};
            opt.record_batch_size(cfg_.record_batch_size_);
            opt.record_batch_in_bytes(cfg_.record_batch_in_bytes_);
            opt.use_fixed_size_binary_for_char(cfg_.arrow_use_fixed_size_binary_for_char_);
            opt.time_unit(cfg_.time_unit_kind_);
            file_writer_ = file::arrow_writer::open(parent_->meta(), p.string(), opt);
        } else {
            file::parquet_writer_option opt{};
            opt.time_unit(cfg_.time_unit_kind_);
            file_writer_ = file::parquet_writer::open(parent_->meta(), p.string(), opt);
        }
        if (! file_writer_) {
            VLOG_LP(log_error) << "dump file creation failed on path " << p.string();
            return false;
        }

        constexpr static std::size_t max_row_groups_per_file = 16;
        max_recs_per_file_ = max_row_groups_per_file * file_writer_->row_group_max_records();
        if(max_recs_per_file_ != 0 && cfg_.max_records_per_file_ != 0) {
            max_recs_per_file_ = std::min(max_recs_per_file_, cfg_.max_records_per_file_);
        } else if (max_recs_per_file_ == 0) {
            max_recs_per_file_ = cfg_.max_records_per_file_;
        } else {
            // use max_records_ set above
        }
    }
    if(auto res = file_writer_->write(rec); ! res) {
        return false;
    }

    if(max_recs_per_file_ != 0 && file_writer_->write_count() >= max_recs_per_file_) {
        // max records of file reached, close current and move to next file
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

void dump_channel_writer::close_file_writer() {
    file_writer_->close();
    write_file_path(file_writer_->path());
    file_writer_.reset();
    ++current_sequence_number_;
}

void dump_channel_writer::flush() {
    if (file_writer_) {
        close_file_writer();
    }
}

}  // namespace jogasaki::executor::io
