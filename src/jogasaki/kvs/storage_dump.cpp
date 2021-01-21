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
#include "storage_dump.h"

#include <cassert>
#include <cstdint>

#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/iterator.h>

namespace jogasaki::kvs {

namespace {

using size_type = std::uint32_t;
inline constexpr size_type EOF_MARK = std::numeric_limits<size_type>::max();

inline std::unique_ptr<storage> storage(database* db, std::string_view key) {
    auto stg = db->get_storage(key);
    if (! stg) {
        stg = db->create_storage(key);
    }
    return stg;
}

class dump_step {
public:
    dump_step(std::ostream& stream, std::string_view storage_key, std::size_t batch_size)
        :
        stream_(stream),
        storage_key_(storage_key),
        batch_size_(batch_size)
    {}

    void operator()(transaction& tx) {
        auto* db = tx.database();
        auto stg = storage(db, storage_key_);

        std::unique_ptr<iterator> it{};
        if (!cont_) {
            cont_ = true;
            check(stg->scan(
                tx,
                "", end_point_kind::unbound,
                "", end_point_kind::unbound,
                it
            ));
        } else {
            check(stg->scan(
                tx,
                last_key_, end_point_kind::exclusive,
                "", end_point_kind::unbound,
                it
            ));
        }

        for (std::size_t i = 1;; ++i) {
            if (!it->next()) {
                eof_ = true;
                storage_dump::append_eof(stream_);
                break;
            }
            std::string_view key{};
            std::string_view value{};
            check(it->key(key));
            check(it->value(value));
            storage_dump::append(stream_, key, value);

            if (batch_size_ > 0 && i >= batch_size_) {
                eof_ = false;
                last_key_ = key;
                break;
            }
        }
    }

    explicit operator bool() {
        return !eof_;
    }

private:
    std::ostream& stream_;
    std::string_view storage_key_{};
    std::size_t batch_size_{};

    std::string last_key_{};
    bool cont_ { false };
    bool eof_ { false };
};

class load_step {
public:
    load_step(std::istream& stream, std::string_view storage_key, std::size_t batch_size)
        :
        stream_(stream),
        storage_key_(storage_key),
        batch_size_(batch_size)
    {}

    void operator()(transaction& tx) {
        auto* db = tx.database();
        auto stg = storage(db, storage_key_);

        for (std::size_t i = 1;; ++i) {
            if (!storage_dump::read_next(stream_, key_buffer_, value_buffer_)) {
                eof_ = true;
                break;
            }
            check(stg->put(
                tx,
                key_buffer_,
                value_buffer_
            ));

            if (batch_size_ > 0 && i >= batch_size_) {
                eof_ = false;
                break;
            }
        }
    }

    explicit operator bool() {
        return !eof_;
    }

private:
    std::istream& stream_;
    std::string_view storage_key_{};
    std::size_t batch_size_{};

    bool eof_ { false };

    std::string key_buffer_{};
    std::string value_buffer_{};
};

template<class Step>
static void process_step(database& db, Step& step) {
    auto tx = db.create_transaction();
    step(*tx);
    check(tx->commit() == status::ok);
}

}  // namespace

void storage_dump::dump(std::ostream& stream, std::string_view storage_name, std::size_t batch_size) {
    dump_step step { stream, storage_name, batch_size };
    do {
        process_step(*db_, step);
    } while (step);
}

void storage_dump::load(std::istream& stream, std::string_view storage_name, std::size_t batch_size) {
    load_step step { stream, storage_name, batch_size };
    do {
        process_step(*db_, step);
    } while (step);
}

void storage_dump::append(std::ostream& stream, std::string_view key, std::string_view value) {
    assert(key.size() != EOF_MARK);  // NOLINT

    auto key_size = static_cast<size_type>(key.size());
    stream.write(reinterpret_cast<char*>(&key_size), sizeof(key_size));  // NOLINT

    auto value_size = static_cast<size_type>(value.size());
    stream.write(reinterpret_cast<char*>(&value_size), sizeof(value_size));  // NOLINT

    stream.write(key.data(), key.size());
    stream.write(value.data(), value.size());
}

void storage_dump::append_eof(std::ostream& stream) {
    auto mark = EOF_MARK;
    stream.write(reinterpret_cast<char*>(&mark), sizeof(mark));  // NOLINT
}

bool storage_dump::read_next(std::istream& stream, std::string& key, std::string& value) {
    size_type key_size;
    stream.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));  // NOLINT
    if (key_size == EOF_MARK) {
        return false;
    }

    size_type value_size;
    stream.read(reinterpret_cast<char*>(&value_size), sizeof(value_size));  // NOLINT

    key.resize(key_size);
    if (key_size > 0) {
        stream.read(key.data(), key_size);
    }
    value.resize(value_size);
    if (value_size > 0) {
        stream.read(value.data(), value_size);
    }
    return true;
}

}
