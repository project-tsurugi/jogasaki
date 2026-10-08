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
#include <memory>
#include <stdexcept>

#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor::process {

TEST(process_io_binding_test, initializes_before_activation) {
    step process{};
    process.relation_io_map(std::make_shared<relation_io_map>());
    auto exchanges = std::make_shared<io_exchange_map>();
    ASSERT_FALSE(process.io_info());
    process.bind_io(exchanges);
    auto metadata = process.io_info();
    ASSERT_TRUE(metadata);
    EXPECT_EQ(exchanges, process.io_exchange_map());
}

TEST(process_io_binding_test, activation_rejects_unprepared_metadata) {
    step process{};
    process.relation_io_map(std::make_shared<relation_io_map>());
    auto exchanges = std::make_shared<io_exchange_map>();
    process.io_exchange_map(exchanges);
    request_context request{};
    EXPECT_THROW(process.activate(request), std::logic_error);
}

TEST(process_io_binding_test, rejects_missing_maps_and_count_mismatch) {
    {
        step process{};
        EXPECT_THROW(process.bind_io({}), std::logic_error);
    }
    {
        step process{};
        process.relation_io_map({});
        EXPECT_THROW(process.bind_io(std::make_shared<io_exchange_map>()), std::logic_error);
    }
    {
        step process{};
        process.relation_io_map(std::make_shared<relation_io_map>());
        auto exchanges = std::make_shared<io_exchange_map>();
        exchange::forward::step output{};
        exchanges->add_output(0, &output);
        EXPECT_THROW(process.bind_io(exchanges), std::logic_error);
    }
}

TEST(process_io_binding_test, rejects_unbound_input_and_output_slots) {
    for (bool bind_input : {false, true}) {
        step process{};
        process.relation_io_map(std::make_shared<relation_io_map>(
            relation_io_map::input_entity_type{{yugawara::binding::factory{}(takatori::plan::forward{}), 0}},
            relation_io_map::output_entity_type{{nullptr, 0}}));
        auto exchanges = std::make_shared<io_exchange_map>();
        exchange::forward::step input{};
        exchanges->add_input(0, bind_input ? &input : nullptr);
        exchanges->add_output(0, nullptr);
        EXPECT_THROW(process.bind_io(exchanges), std::logic_error);
    }
}

TEST(process_io_binding_test, initializes_input_and_output_metadata) {
    step process{};
    process.relation_io_map(std::make_shared<relation_io_map>(
        relation_io_map::input_entity_type{{yugawara::binding::factory{}(takatori::plan::forward{}), 0}},
        relation_io_map::output_entity_type{{nullptr, 0}}));
    auto record_metadata = std::make_shared<meta::record_meta>();
    exchange::forward::step exchange{record_metadata};
    auto exchanges = std::make_shared<io_exchange_map>();
    exchanges->add_input(0, &exchange);
    exchanges->add_output(0, &exchange);
    process.bind_io(exchanges);
    ASSERT_TRUE(process.io_info());
    EXPECT_EQ(record_metadata.get(), process.io_info()->input_at(0).record_meta().get());
    EXPECT_EQ(record_metadata.get(), process.io_info()->output_at(0).meta().get());
}

} // namespace jogasaki::executor::process
