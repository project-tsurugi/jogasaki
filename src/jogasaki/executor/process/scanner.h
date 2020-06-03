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
#include <jogasaki/storage/storage_context.h>
#include <jogasaki/storage/transaction_context.h>
#include "scan_info.h"

namespace jogasaki::executor::process {

/**
 * @brief scanner
 */
class scanner {
public:
    /**
     * @brief create empty object
     */
    scanner() = default;

    /**
     * @brief create new object
     */
    scanner(std::shared_ptr<scan_info> info,
            std::shared_ptr<storage::storage_context> storage,
            std::shared_ptr<meta::record_meta> meta,
            accessor::record_ref buf) :
            info_(std::move(info)),
            storage_(std::move(storage)),
            meta_(std::move(meta)),
            buf_(buf) {
    }

    void open() {
        tx_ = storage_->create_transaction();
//        if (auto res = sharksfin::content_scan(tx_->handle(), &iterator_); res != sharksfin::StatusCode::OK) {
//            takatori::util::fail();
//        }
    }

    bool next() {
        // TODO read from sharksfin
        auto offset_c1 = meta_->value_offset(0);
        auto offset_c2 = meta_->value_offset(1);
        struct rec {
            std::int64_t c1;
            double c2;
        };
        thread_local rec r{};
        std::memcpy(buf_.data(), &r, sizeof(rec));
        r.c1++;
        r.c2++;
    }

    void close() {
        tx_->commit();
    }
private:
    std::shared_ptr<scan_info> info_{};
    std::shared_ptr<storage::storage_context> storage_{};
    std::shared_ptr<meta::record_meta> meta_{};
    accessor::record_ref buf_{};
    std::shared_ptr<storage::transaction_context> tx_{};

    sharksfin::IteratorHandle iterator_{};
};

}


