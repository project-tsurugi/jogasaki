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
#pragma once

#include <memory>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/result_set_iterator.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::api::impl {

using takatori::util::maybe_shared_ptr;

/**
 * @brief iterator over result records
 */
class result_set_iterator : public api::result_set_iterator {
public:

    result_set_iterator(
        data::result_store::iterator it,
        data::result_store::iterator end,
        maybe_shared_ptr<meta::record_meta> const& meta
    );

    /**
     * @brief copy construct
     */
    result_set_iterator(result_set_iterator const&) = delete;

    /**
     * @brief move construct
     */
    result_set_iterator(result_set_iterator &&) = default;

    /**
     * @brief copy assign
     */
    result_set_iterator& operator=(result_set_iterator const&) = delete;

    /**
     * @brief move assign
     */
    result_set_iterator& operator=(result_set_iterator &&) = default;

    /**
     * @brief destruct result_set_iterator
     */
    ~result_set_iterator() override = default;

    [[nodiscard]] bool has_next() const noexcept override;  //NOLINT(modernize-use-nodiscard)

    /**
     * @brief move the iterator to the next row returning the current
     * @return the current row
     * @throw Exception on error
     */
    [[nodiscard]] record* next() override;

private:
    data::result_store::iterator it_{};
    data::result_store::iterator end_{};
    impl::record record_{};
};

}
