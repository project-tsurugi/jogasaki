/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <chrono>
#include <memory>
#include <optional>
#include <vector>

#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/udf/generic_record.h>

namespace jogasaki::data {

/**
 * @brief adapter class that wraps generic_record_stream to provide any_sequence_stream interface.
 * @details this adapter bridges the UDF world (generic_record_stream) and the jogasaki world (any_sequence_stream).
 *          it converts generic_record to any_sequence by mapping record fields to any values.
 */
class udf_any_sequence_stream : public any_sequence_stream {
public:
    /**
     * @brief constructs a new adapter with the specified generic_record_stream.
     * @param udf_stream the underlying UDF stream
     * @param column_types the types of the columns in the result table
     */
    udf_any_sequence_stream(
        std::unique_ptr<plugin::udf::generic_record_stream> udf_stream,
        std::vector<meta::field_type> column_types
    );

    /**
     * @brief attempts to retrieve the next record from the stream without blocking.
     * @param seq the sequence to store the retrieved data
     * @return status_type::ok if a record was successfully retrieved
     * @return status_type::error if an error occurred
     * @return status_type::end_of_stream if the end of the stream has been reached
     * @return status_type::not_ready if the next record is not yet available
     */
    [[nodiscard]] status_type try_next(any_sequence& seq) override;

    /**
     * @brief retrieves the next record from the stream, waiting up to the specified timeout.
     * @param seq the sequence to store the retrieved data
     * @param timeout the maximum duration to wait for the next record, or std::nullopt to wait indefinitely
     * @return status_type::ok if a record was successfully retrieved
     * @return status_type::error if an error occurred
     * @return status_type::end_of_stream if the end of the stream has been reached
     * @return status_type::not_ready if the operation timed out before a record could be retrieved
     */
    [[nodiscard]] status_type next(any_sequence& seq, std::optional<std::chrono::milliseconds> timeout) override;

    /**
     * @brief closes the stream and releases associated resources.
     */
    void close() override;

private:
    std::unique_ptr<plugin::udf::generic_record_stream> udf_stream_;
    std::vector<meta::field_type> column_types_;

    /**
     * @brief converts a generic_record to any_sequence.
     * @param record the generic_record to convert
     * @param seq the any_sequence to store the converted data
     * @return true if conversion succeeded
     * @return false if conversion failed
     */
    bool convert_record_to_sequence(plugin::udf::generic_record const& record, any_sequence& seq);
};

}  // namespace jogasaki::data
