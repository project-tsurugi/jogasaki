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

#include <jogasaki/api/result_set.h>
#include <jogasaki/data/iteratable_record_store.h>

namespace jogasaki::api {

class result_set::impl {
public:
    explicit impl(std::shared_ptr<data::iteratable_record_store> store) noexcept : store_(std::move(store)) {
        //FIXME temp. implementation for client access
        refs_.reserve(store_->count());
        auto sz = store_->record_size();
        for(auto it : *store_) {
            refs_.emplace_back(it, sz);
        }
    }

    iterator begin();
    iterator end();
    void close();

private:
    std::shared_ptr<data::iteratable_record_store> store_{};
    std::vector<accessor::record_ref> refs_{}; //FIXME
};

}
