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

#include <iomanip>

#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/accessor/record_ref.h>

namespace jogasaki::utils {

using takatori::util::fail;
using takatori::util::maybe_shared_ptr;

/**
 * @brief parquet file writer
 */
class parquet_writer {
public:
    explicit parquet_writer(maybe_shared_ptr<meta::external_record_meta> meta) :
        meta_(std::move(meta))
    {}

    bool init(std::string_view path) {
        path_ = path;
        return true;
    }

    bool write(accessor::record_ref ref) {

    }

    bool close() {

    }

    static std::shared_ptr<parquet_writer> open(maybe_shared_ptr<meta::external_record_meta> meta, std::string_view path) {
        auto ret = std::make_shared<parquet_writer>(std::move(meta));
        if(ret->init(path)) {
            return ret;
        }
        return {};
    }

private:
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    std::string path_{};
};


}

}
