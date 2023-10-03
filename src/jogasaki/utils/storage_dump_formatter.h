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

#include <sstream>
#include <takatori/util/fail.h>

#include <jogasaki/kvs/storage_dump.h>
#include <jogasaki/utils/binary_printer.h>

namespace jogasaki::utils {

using takatori::util::fail;

namespace details {

class key_value_streambuf : public std::streambuf {
public:
    using size_type = kvs::storage_dump::size_type;

    key_value_streambuf() = default;

    void set_output(std::ostream* out) noexcept {
        out_ = out;
    }

protected:
    std::streamsize xsputn(char const* s, std::streamsize count) override {
        std::streamsize ret{count};
        switch(write_count_ % 4) {
            case 0:
                if (count == sizeof(kvs::storage_dump::EOF_MARK) &&
                    kvs::storage_dump::EOF_MARK == *reinterpret_cast<size_type const*>(s)) {  //NOLINT
                    if (out_) {
                        auto str = buf_.str();
                        out_->write(str.data(), str.size());
                    }
                }
                if (count != sizeof(size_type)) fail();
                buf_.clear();
                key_len_ = *reinterpret_cast<size_type const*>(s); //NOLINT
                break;
            case 1:
                if (count != sizeof(size_type)) fail();
                value_len_ = *reinterpret_cast<size_type const*>(s); //NOLINT
                break;
            case 2: {
                if (count != key_len_) fail();
                utils::binary_printer p{s, static_cast<std::size_t>(count)};
                buf_ << "key:";
                buf_ << p;
                break;
            }
            case 3: {
                if (count != value_len_) fail();
                utils::binary_printer p{s, static_cast<std::size_t>(count)};
                buf_ << " value:";
                buf_ << p;
                buf_ << std::endl;
                break;
            }
        }
        ++write_count_;
        return ret;
    }

private:
    std::size_t write_count_{};
    size_type key_len_{};
    size_type value_len_{};
    std::stringstream buf_{};
    std::ostream* out_{};
};

}

/**
 * @brief debug support to print dump data in readable format
 */
class storage_dump_formatter {
public:
    storage_dump_formatter() = default;

    std::ostream connect(std::ostream& os) noexcept {
        out_ = std::addressof(os);
        buf_.set_output(std::addressof(os));
        return std::ostream(std::addressof(buf_));
    }

    std::ostream* disconnect() noexcept {
        auto p = out_;
        out_ = nullptr;
        buf_.set_output(nullptr);
        return p;
    }

    friend std::ostream operator<<(std::ostream& os, storage_dump_formatter& fmt) noexcept {
        fmt.connect(os);
        return std::ostream(std::addressof(fmt.buf_));
    }

private:
    std::ostream* out_{};
    details::key_value_streambuf buf_{};
};

}
