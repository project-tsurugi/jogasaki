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

#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <vector>
#include <string_view>

/**
 * @brief mock hayatsuki objects that do nothing on callback
 */
namespace hayatsuki {

/**
 * @brief operation type for log entry
 */
enum class LogOperation : std::uint32_t {
    UNKNOWN = 0U,
    INSERT,
    UPDATE,
    DELETE,
    UPSERT,
};

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(LogOperation value) {
    switch (value) {
        case LogOperation::UNKNOWN:
            return "UNKNOWN";
        case LogOperation::INSERT:
            return "INSERT";
        case LogOperation::UPDATE:
            return "UPDATE";
        case LogOperation::DELETE:
            return "DELETE";
        case LogOperation::UPSERT:
            return "UPSERT";
    }
    std::abort();
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream
 * @param value the source enum value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, LogOperation value) {
    return out << to_string_view(value);
}

class log_record {
public:
    constexpr log_record(LogOperation, std::string_view, std::string_view, std::uint64_t, std::uint64_t, std::uint64_t)  //NOLINT
    {}
};

class Collector {  //NOLINT
public:
    Collector() = default;
    virtual ~Collector() = default;
    virtual int init(int max_channels) = 0;
    virtual int finish() = 0;
    virtual int write_message(int channel_number, std::vector<log_record>& rec) = 0;
    virtual std::string get_error_message(int error_code) = 0;
    virtual std::uint64_t get_drained_version() = 0;
};

class ShirakamiCollector : public Collector {
public:
    ShirakamiCollector() = default;
    int init(int) override { return 0; }  //NOLINT
    int finish() override { return 0; }
    int write_message(int, std::vector<log_record>&) override { return 0; }  //NOLINT
    std::string get_error_message(int) override { return {}; }  //NOLINT
    std::uint64_t get_drained_version() override { return {}; }
};

}


