/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::kvs {

/**
 * @brief utilities of save/load contents of storages.
 */
class storage_dump {
public:
    using size_type = std::uint32_t;

    static constexpr size_type EOF_MARK = std::numeric_limits<size_type>::max();

    /**
     * @brief creates a new instance.
     * @param dbh the database handle
     */
    explicit storage_dump(jogasaki::kvs::database& db) noexcept : db_(std::addressof(db)) {}

    /**
     * @brief dumps contents of the target storage into the given output stream.
     * @param stream the destination output stream
     * @param storage_name the target storage
     * @param batch_size the max number of entries to be processed in each transaction,
     *    or 0 to process all entries in one transaction
     * @return status::ok when successful
     * @return any error otherwise
     */
    status dump(std::ostream& stream, std::string_view storage_name, std::size_t batch_size = 0);

    /**
     * @brief loads contents of the target storage from the given input stream.
     * @param stream the source input stream
     * @param storage_name the target storage
     * @param batch_size the max number of entries to be processed in each transaction,
     *    or 0 to process all entries in one transaction
     * @return status::ok when successful
     * @return any error otherwise
     */
    status load(std::istream& stream, std::string_view storage_name, std::size_t batch_size = 0);

    /**
     * @brief appends a dump entry into the given stream.
     * @param stream the destination output stream
     * @param key the key
     * @param value the value
     */
    static void append(std::ostream& stream, std::string_view key, std::string_view value);

    /**
     * @brief appends an EOF mark into the given stream.
     * @param stream the destination output stream
     */
    static void append_eof(std::ostream& stream);

    /**
     * @brief obtains the next entry from the given stream that contains dump key value pairs by append().
     * @param stream the source input stream.
     * @param key [OUT] the key buffer
     * @param value [OUT] the value buffer
     * @return true if successfully obtained
     * @return false if saw end of file
     */
    static bool read_next(std::istream& stream, std::string& key, std::string& value);

private:
    database* db_{};
};

/**
 * @brief checks status code or raise an exception if it is not OK.
 * @param result the status code
 */
inline void check(status st) {
    if (st != status::ok) {
        fail_with_exception();
    }
}

/**
 * @brief checks return value or raise an exception if it is not OK.
 * @param result the return value from executed function
 */
inline void check(bool result) {
    if (! result) {
        fail_with_exception();
    }
}

}
