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
#include <jogasaki/executor/process/impl/process_executor.h>

#include <string>

#include <gtest/gtest.h>

#include <jogasaki/test_root.h>

#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/executor/process/mock/process_executor.h>
#include <jogasaki/executor/process/mock/record_writer.h>
#include <jogasaki/executor/process/mock/processor.h>
#include <jogasaki/mock/basic_record.h>

namespace jogasaki::executor::process::impl {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace testing;
using namespace jogasaki::memory;
using namespace boost::container::pmr;

using namespace jogasaki::mock;

class process_executor_test : public test_root {
public:
    using record_type = mock::basic_record_reader::record_type;
    using kind = meta::field_type_kind;
    void SetUp() override {
        reader_ = std::make_shared<mock::basic_record_reader>(records_, records_[0].record_meta());
        reader_container r{reader_.get()};
        downstream_writer_ = mock::create_writer_shared<kind::int8, kind::float8>();
        external_writer_ = mock::create_writer_shared<kind::int8, kind::float8>();
        contexts_.emplace_back(std::make_shared<mock::task_context>(
            std::vector<reader_container>{r},
            std::vector<std::shared_ptr<executor::record_writer>>{downstream_writer_},
            external_writer_,
            std::shared_ptr<abstract::scan_info>{}
        ));
    }
    mock::basic_record_reader::records_type records_{
        create_record<kind::int8, kind::float8>(1, 1.0),
        create_record<kind::int8, kind::float8>(2, 2.0),
        create_record<kind::int8, kind::float8>(3, 3.0),
    };
    std::shared_ptr<mock::basic_record_reader> reader_{};
    std::vector<std::shared_ptr<abstract::task_context>> contexts_{};
    std::shared_ptr<mock::basic_record_writer> downstream_writer_{};
    std::shared_ptr<mock::basic_record_writer> external_writer_{};
    std::shared_ptr<mock::processor> proc_ = std::make_shared<mock::processor>();
};

using kind = meta::field_type_kind;

TEST_F(process_executor_test, basic) {
    process_executor exec{proc_, contexts_};
    ASSERT_EQ(abstract::status::completed, exec.run());
    auto writer = downstream_writer_.get();
    auto ewriter = external_writer_.get();
    EXPECT_EQ(4, reader_->num_calls_next_record());
    EXPECT_EQ(3, writer->size());
    EXPECT_EQ(3, ewriter->size());
    EXPECT_TRUE(reader_->is_released());
    EXPECT_TRUE(writer->is_released());
    EXPECT_TRUE(ewriter->is_released());
}

TEST_F(process_executor_test, default_factory) {
    abstract::process_executor_factory f = impl::default_process_executor_factory();
    auto executor = f(proc_, contexts_);
    ASSERT_EQ(abstract::status::completed, executor->run());
    auto writer = downstream_writer_.get();
    auto ewriter = external_writer_.get();
    EXPECT_EQ(4, reader_->num_calls_next_record());
    EXPECT_EQ(3, writer->size());
    EXPECT_EQ(3, ewriter->size());
    EXPECT_TRUE(reader_->is_released());
    EXPECT_TRUE(writer->is_released());
    EXPECT_TRUE(ewriter->is_released());
}

TEST_F(process_executor_test, custom_factory) {
    // custom factory discarding passed contexts and use customized one
    mock::basic_record_reader::records_type records{
        create_record<kind::int8, kind::float8>(1, 1.0),
    };
    auto reader = std::make_shared<mock::basic_record_reader>(records, records[0].record_meta());
    reader_container r{reader.get()};
    std::vector<std::shared_ptr<abstract::task_context>> custom_contexts{};
    custom_contexts.emplace_back(std::make_shared<mock::task_context>(
        std::vector<reader_container>{r},
        std::vector<std::shared_ptr<executor::record_writer>>{downstream_writer_},
        external_writer_,
        std::shared_ptr<abstract::scan_info>{}
    ));
    abstract::process_executor_factory f = [&](
        std::shared_ptr<abstract::processor> processor,
        std::vector<std::shared_ptr<abstract::task_context>> contexts
    ) {
        return std::make_shared<process_executor>(std::move(processor), std::move(custom_contexts));
    };

    auto executor = f(proc_, contexts_);
    ASSERT_EQ(abstract::status::completed, executor->run());
    auto writer = downstream_writer_.get();
    auto ewriter = external_writer_.get();
    EXPECT_EQ(0, reader_->num_calls_next_record());
    EXPECT_EQ(2, reader->num_calls_next_record());
    EXPECT_EQ(1, writer->size());
    EXPECT_EQ(1, ewriter->size());
    EXPECT_FALSE(reader_->is_released()); // not used
    EXPECT_TRUE(reader->is_released());
    EXPECT_TRUE(writer->is_released());
    EXPECT_TRUE(ewriter->is_released());
}

}

