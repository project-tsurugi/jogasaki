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

#include <vector>

#include <takatori/util/sequence_view.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/data/record_store.h>
#include "scan_info.h"

namespace jogasaki::executor::process {

/**
 * @brief emitter
 */
class emitter {
public:
    /**
     * @brief create empty object
     */
    emitter() = default;

    /**
     * @brief create new object
     */
    emitter(
            std::shared_ptr<meta::record_meta> meta,
            std::shared_ptr<data::record_store> store) :
            meta_(std::move(meta)),
            store_(std::move(store)) {
    }

    void emit(accessor::record_ref record) {
        store_->append(record);
    }

private:
    std::shared_ptr<meta::record_meta> meta_{};
    std::shared_ptr<data::record_store> store_{};
};

}


