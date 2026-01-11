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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include <takatori/util/exception.h>

#include <jogasaki/data/any.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/lob/lob_data_provider.h>
#include <jogasaki/lob/lob_reference.h>

namespace jogasaki::testing {

class data_relay_client;

/**
 * @brief Download LOB data from the data relay service.
 * @tparam T the LOB reference type (e.g., lob::clob_reference or lob::blob_reference)
 * @param ectx the evaluator context
 * @param in the input data containing the LOB reference
 * @param client the data relay client
 * @param reference_tag the reference tag to use for verification (optional)
 * @return the downloaded LOB data as a string
 */
template <class T>
std::string download_lob(executor::expr::evaluator_context& ectx, data::any in, data_relay_client& client, std::optional<std::uint64_t> reference_tag = std::nullopt) {
    auto const& ref = in.to<T>();
    std::size_t blob_id = ref.object_id();
    auto provider = ref.provider();

    auto s = ectx.blob_session()->get_or_create();
    if (! s) {
        takatori::util::throw_exception(std::runtime_error(""));
    }
    std::uint64_t session_id = s->session_id();

    // Determine storage_id based on the data provider
    // SESSION_STORAGE_ID = 0, LIMESTONE_BLOB_STORE = 1
    std::uint64_t storage_id = (provider == lob::lob_data_provider::datastore) ? 1 : 0;

    auto tag = reference_tag ? reference_tag.value() : s->compute_tag(blob_id);
    return client.get_blob(session_id, storage_id, blob_id, tag);
}

/**
 * @brief Upload LOB data to the data relay service.
 * @tparam T the LOB reference type (e.g., lob::clob_reference or lob::blob_reference)
 * @param ectx the evaluator context
 * @param in the input data to upload
 * @param client the data relay client
 * @return the LOB reference as data::any
 */
template <class T>
data::any upload_lob(executor::expr::evaluator_context& ectx, std::string const& in, data_relay_client& client) {
    auto s = ectx.blob_session()->get_or_create();
    if (! s) {
        takatori::util::throw_exception(std::runtime_error(""));
    }
    std::uint64_t session_id = s->session_id();

    auto [blob_id, storage_id, tag] = client.put_blob(session_id, in);
    if (blob_id == 0) {
        takatori::util::throw_exception(
            std::runtime_error("put_blob() failed session_id:" + std::to_string(session_id))
        );
    }

    // The gRPC Put returns storage_id=0 (SESSION_STORAGE_ID)
    // but we need the blob to be accessible from the session
    return data::any{std::in_place_type<T>, T{blob_id, lob::lob_data_provider::relay_service_session}.reference_tag(tag)};
}

}  // namespace jogasaki::testing
