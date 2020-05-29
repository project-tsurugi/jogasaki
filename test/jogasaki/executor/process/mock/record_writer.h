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

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/record_writer.h>

namespace jogasaki::executor::process::mock {

class record_writer : public executor::record_writer {
public:

    explicit record_writer(std::shared_ptr<meta::record_meta> meta) :
            meta_(std::move(meta)),
            offset_c1_(meta_->value_offset(0)),
            offset_c2_(meta_->value_offset(1))
    {}

    bool write(accessor::record_ref rec) override {
        records_.emplace_back(
                rec.get_value<std::int64_t>(offset_c1_),
                rec.get_value<double>(offset_c2_)
        );
        return false;
    }

    void flush() override {
        // no-op
    }

    void release() override {
        released_ = true;
    }

    std::shared_ptr<meta::record_meta> meta_{};
    std::vector<data::record> records_{};
    std::size_t offset_c1_{};
    std::size_t offset_c2_{};
    bool released_{false};
};

}

