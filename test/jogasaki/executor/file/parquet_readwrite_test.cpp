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
#include <cstddef>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/file/parquet_reader.h>
#include <jogasaki/executor/file/parquet_writer.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_utils/temporary_folder.h>

namespace jogasaki::executor::file {

using namespace std::chrono_literals;

using kind = meta::field_type_kind;
using accessor::text;
using takatori::datetime::date;
using takatori::datetime::time_of_day;
using takatori::datetime::time_point;

class parquet_readwrite_test : public ::testing::Test {
public:
    void SetUp() override {
        temporary_.prepare();
    }
    void TearDown() override {
        temporary_.clean();
    }

    std::string path() {
        return temporary_.path();
    }

    test::temporary_folder temporary_{};  //NOLINT

    void test_time_point_time_unit(time_unit_kind kind, time_point expected, time_point input);
    void test_rw_decimal(meta::field_type& ftype, std::string_view filename, mock::basic_record& rec);
};

TEST_F(parquet_readwrite_test, simple) {
    boost::filesystem::path p{path()};
    p = p / "simple.parquet";
    auto rec = mock::create_nullable_record<kind::int8, kind::float8>(10, 100.0);
    parquet_writer_option opt{};
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1"}
    ), p.string(), opt);
    ASSERT_TRUE(writer);

    writer->write(rec.ref());
    writer->write(rec.ref());
    writer->close();
    EXPECT_EQ(p.string(), writer->path());
    EXPECT_EQ(2, writer->write_count());
    ASSERT_LT(0, boost::filesystem::file_size(p));

    auto reader = parquet_reader::open(p.string());
    ASSERT_TRUE(reader);
    auto meta = reader->meta();
    ASSERT_EQ(2, meta->field_count());
    EXPECT_EQ("C0", meta->field_name(0));
    EXPECT_EQ("C1", meta->field_name(1));
    EXPECT_TRUE(meta->nullable(0));
    EXPECT_TRUE(meta->nullable(1));
    EXPECT_EQ(meta::field_type_kind::int8, meta->at(0).kind());
    EXPECT_EQ(meta::field_type_kind::float8, meta->at(1).kind());
    {
        accessor::record_ref ref{};
        ASSERT_TRUE(reader->next(ref));
        EXPECT_EQ(rec, mock::basic_record(ref, meta->origin()));
    }
    {
        accessor::record_ref ref{};
        ASSERT_TRUE(reader->next(ref));
        EXPECT_EQ(rec, mock::basic_record(ref, meta->origin()));
    }
    {
        accessor::record_ref ref{};
        ASSERT_FALSE(reader->next(ref));
    }
    EXPECT_EQ(2, reader->read_count());
    EXPECT_EQ(p.string(), reader->path());
    EXPECT_TRUE(reader->close());
}

TEST_F(parquet_readwrite_test, basic_types1) {
    boost::filesystem::path p{path()};
    p = p / "basic_types1.parquet";
    auto rec = mock::create_nullable_record<kind::int4, kind::int8, kind::float4, kind::float8, kind::character>(1, 10, 100.0, 1000.0, accessor::text("10000"));
    parquet_writer_option opt{};
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1", "C2", "C3", "C4"}
        ), p.string(), opt);
    ASSERT_TRUE(writer);

    writer->write(rec.ref());
    writer->close();
    ASSERT_LT(0, boost::filesystem::file_size(p));

    auto reader = parquet_reader::open(p.string());
    ASSERT_TRUE(reader);
    auto meta = reader->meta();
    ASSERT_EQ(5, meta->field_count());
    EXPECT_EQ(meta::field_type_kind::int4, meta->at(0).kind());
    EXPECT_EQ(meta::field_type_kind::int8, meta->at(1).kind());
    EXPECT_EQ(meta::field_type_kind::float4, meta->at(2).kind());
    EXPECT_EQ(meta::field_type_kind::float8, meta->at(3).kind());
    EXPECT_EQ(meta::field_type_kind::character, meta->at(4).kind());
    {
        accessor::record_ref ref{};
        ASSERT_TRUE(reader->next(ref));
        EXPECT_EQ(rec, mock::basic_record(ref, meta->origin()));
    }
    EXPECT_TRUE(reader->close());
}

TEST_F(parquet_readwrite_test, temporal_types) {
    boost::filesystem::path p{path()};
    p = p / "temporal_types.parquet";
    auto rec = mock::typed_nullable_record<
        kind::date, kind::time_of_day, kind::time_point
    >(
        std::tuple{
            meta::field_type{meta::field_enum_tag<kind::date>},
            meta::field_type{std::make_shared<meta::time_of_day_field_option>()},
            meta::field_type{std::make_shared<meta::time_point_field_option>()},
        },
        {
            runtime_t<meta::field_type_kind::date>(),
            runtime_t<meta::field_type_kind::time_of_day>(),
            runtime_t<meta::field_type_kind::time_point>(),
        }
    );
    parquet_writer_option opt{};
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1", "C2"}
        ), p.string(), opt);
    ASSERT_TRUE(writer);

    EXPECT_TRUE(writer->write(rec.ref()));
    EXPECT_TRUE(writer->close());

    ASSERT_LT(0, boost::filesystem::file_size(p));

    auto reader = parquet_reader::open(p.string());
    ASSERT_TRUE(reader);
    auto meta = reader->meta();
    ASSERT_EQ(3, meta->field_count());
    EXPECT_EQ(meta::field_type_kind::date, meta->at(0).kind());
    EXPECT_EQ(meta::field_type_kind::time_of_day, meta->at(1).kind());
    EXPECT_EQ(meta::field_type_kind::time_point, meta->at(2).kind());
    {
        accessor::record_ref ref{};
        ASSERT_TRUE(reader->next(ref));
        EXPECT_EQ(rec, mock::basic_record(ref, meta->origin()));
    }
    EXPECT_TRUE(reader->close());
}

void parquet_readwrite_test::test_time_point_time_unit(time_unit_kind kind, time_point expected, time_point input) {
    boost::filesystem::path p{path()};
    p = p / "time_point_time_unit.parquet";
    auto rec = mock::typed_nullable_record<kind::time_point>(std::tuple{meta::time_point_type(false)}, {input});

    parquet_writer_option opt{};
    opt.time_unit(kind);
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0"}
        ), p.string(), opt);
    ASSERT_TRUE(writer);

    EXPECT_TRUE(writer->write(rec.ref()));
    EXPECT_TRUE(writer->close());

    ASSERT_LT(0, boost::filesystem::file_size(p));

    auto reader = parquet_reader::open(p.string());
    ASSERT_TRUE(reader);
    auto meta = reader->meta();
    ASSERT_EQ(1, meta->field_count());
    EXPECT_EQ(meta::field_type_kind::time_point, meta->at(0).kind());
    {
        accessor::record_ref ref{};
        ASSERT_TRUE(reader->next(ref));
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::time_point>(std::tuple{meta::time_point_type(false)}, {expected})),
            mock::basic_record(ref, meta->origin())
        );
    }
    EXPECT_TRUE(reader->close());
}

TEST_F(parquet_readwrite_test, time_point_time_unit_ns) {
    test_time_point_time_unit(
        time_unit_kind::nanosecond,
        time_point{date{2000, 1, 1}, time_of_day{1, 2, 3, 456789012ns}},
        time_point{date{2000, 1, 1}, time_of_day{1, 2, 3, 456789012ns}}
    );
}
TEST_F(parquet_readwrite_test, time_point_time_unit_us) {
    test_time_point_time_unit(
        time_unit_kind::microsecond,
        time_point{date{2000, 1, 1}, time_of_day{1, 2, 3, 456789000ns}},
        time_point{date{2000, 1, 1}, time_of_day{1, 2, 3, 456789012ns}}
    );
}
TEST_F(parquet_readwrite_test, time_point_time_unit_ms) {
    test_time_point_time_unit(
        time_unit_kind::millisecond,
        time_point{date{2000, 1, 1}, time_of_day{1, 2, 3, 456000000ns}},
        time_point{date{2000, 1, 1}, time_of_day{1, 2, 3, 456789012ns}}
    );
}

// parquet does not support the second unit, so no testcase for it

void parquet_readwrite_test::test_rw_decimal(meta::field_type& ftype, std::string_view filename, mock::basic_record& rec) {
    boost::filesystem::path p{path()};
    p = p / std::string{filename};

    parquet_writer_option opt{};
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0"}
        ), p.string(), opt);
    ASSERT_TRUE(writer);

    EXPECT_TRUE(writer->write(rec.ref()));
    EXPECT_TRUE(writer->close());
    ASSERT_LT(0, boost::filesystem::file_size(p));

    auto reader = parquet_reader::open(p.string());
    ASSERT_TRUE(reader);
    auto meta = reader->meta();
    ASSERT_EQ(1, meta->field_count());
    EXPECT_EQ(meta::field_type_kind::decimal, meta->at(0).kind());
    {
        accessor::record_ref ref{};
        ASSERT_TRUE(reader->next(ref));
        ASSERT_EQ(rec, mock::basic_record(ref, meta->origin()));
    }
    EXPECT_TRUE(reader->close());
}

TEST_F(parquet_readwrite_test, decimal) {
    auto fm = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)};
    {
        SCOPED_TRACE("read/write 1.230");
        auto rec = mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(1, 0, 1230, -3)});
        test_rw_decimal(fm, "decimal.parquet", rec);
    }
}

TEST_F(parquet_readwrite_test, decimal_max_values) {
    auto fm = meta::field_type{std::make_shared<meta::decimal_field_option>(38, 37)};
    {
        SCOPED_TRACE("-9.99....9 (38 digits)");
        auto rec = mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(-1, 0x4B3B4CA85A86C47AUL, 0x098A223FFFFFFFFFUL, -37)});
        test_rw_decimal(fm, "decimal_max_values_0.parquet", rec);
    }
    {
        SCOPED_TRACE("-9.99....8 (38 digits)");
        auto rec = mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(-1, 0x4B3B4CA85A86C47AUL, 0x098A223FFFFFFFFEUL, -37)});
        test_rw_decimal(fm, "decimal_max_values_1.parquet", rec);
    }
    {
        SCOPED_TRACE("+9.99....8 (38 digits)");
        auto rec = mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(1, 0x4B3B4CA85A86C47AUL, 0x098A223FFFFFFFFEUL, -37)});
        test_rw_decimal(fm, "decimal_max_values_2.parquet", rec);
    }
    {
        SCOPED_TRACE("+9.99....9 (38 digits)");
        auto rec = mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(1, 0x4B3B4CA85A86C47AUL, 0x098A223FFFFFFFFFUL, -37)});
        test_rw_decimal(fm, "decimal_max_values_3.parquet", rec);
    }
}

TEST_F(parquet_readwrite_test, nulls) {
    boost::filesystem::path p{path()};
    p = p / "nulls.parquet";
    auto rec0 = mock::create_nullable_record<kind::int8, kind::float8>(10, 100.0);
    auto rec1 = mock::create_nullable_record<kind::int8, kind::float8>({20, 200.0}, {true, true});
    auto rec2 = mock::create_nullable_record<kind::int8, kind::float8>(30, 300.0);
    parquet_writer_option opt{};
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec0.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1"}
        ), p.string(), opt);
    ASSERT_TRUE(writer);

    writer->write(rec0.ref());
    writer->write(rec1.ref());
    writer->write(rec2.ref());
    writer->close();
    ASSERT_LT(0, boost::filesystem::file_size(p));

    auto reader = parquet_reader::open(p.string());
    ASSERT_TRUE(reader);
    auto meta = reader->meta();
    ASSERT_EQ(2, meta->field_count());
    {
        accessor::record_ref ref{};
        ASSERT_TRUE(reader->next(ref));
        EXPECT_EQ(rec0, mock::basic_record(ref, meta->origin()));
        std::cerr << "rec0: " << ref << *meta->origin() << std::endl;
    }
    {
        accessor::record_ref ref{};
        ASSERT_TRUE(reader->next(ref));
        EXPECT_EQ(rec1, mock::basic_record(ref, meta->origin()));
        std::cerr << "rec1: " << ref << *meta->origin() << std::endl;
    }
    {
        accessor::record_ref ref{};
        ASSERT_TRUE(reader->next(ref));
        EXPECT_EQ(rec2, mock::basic_record(ref, meta->origin()));
        std::cerr << "rec2: " << ref << *meta->origin() << std::endl;
    }
    {
        accessor::record_ref ref{};
        ASSERT_FALSE(reader->next(ref));
    }
    EXPECT_TRUE(reader->close());
}

TEST_F(parquet_readwrite_test, generate_decimal_sample) {
    auto fm0 = meta::field_type{std::make_shared<meta::decimal_field_option>(6, 3)};
    auto fm1 = meta::field_type{std::make_shared<meta::decimal_field_option>(4, 1)};
    auto fm2 = meta::field_type{std::make_shared<meta::decimal_field_option>(20, 0)};

    auto rec = mock::typed_nullable_record<kind::decimal, kind::decimal, kind::decimal>(
        std::tuple{fm0, fm1, fm2},
        {
            runtime_t<meta::field_type_kind::decimal>(1, 0, 0, 0),
            runtime_t<meta::field_type_kind::decimal>(1, 0, 0, 0),
            runtime_t<meta::field_type_kind::decimal>(1, 0, 0, 0),
        }
    );
    auto null_rec = mock::typed_nullable_record<kind::decimal, kind::decimal, kind::decimal>(
        std::tuple{fm0, fm1, fm2},
        {
            runtime_t<meta::field_type_kind::decimal>(1, 0, 0, 0),
            runtime_t<meta::field_type_kind::decimal>(1, 0, 0, 0),
            runtime_t<meta::field_type_kind::decimal>(1, 0, 0, 0),
        },
        { true, true, true }
    );

    boost::filesystem::path p{path()};
    p = p / "decimals.parquet";
    parquet_writer_option opt{};
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"decimal_6_3_f", "decimal_4_1_f", "decimal_20_0_f"}
        ), p.string(), opt);
    ASSERT_TRUE(writer);

    writer->write(null_rec.ref());
    for(std::size_t i=0; i < 500; ++i) {
        auto rec = mock::typed_nullable_record<kind::decimal, kind::decimal, kind::decimal>(
            std::tuple{fm0, fm1, fm2},
            {
                runtime_t<meta::field_type_kind::decimal>(1, 0, i, 0),
                runtime_t<meta::field_type_kind::decimal>(1, 0, i, 0),
                runtime_t<meta::field_type_kind::decimal>(1, 0, i, 0),
            }
        );
        writer->write(rec.ref());
    }
    writer->close();
    ASSERT_LT(0, boost::filesystem::file_size(p));

    auto reader = parquet_reader::open(p.string());
    ASSERT_TRUE(reader);
    auto meta = reader->meta();
    ASSERT_EQ(3, meta->field_count());
    {
        accessor::record_ref ref{};
        while(reader->next(ref)) {
            std::cerr << "rec: " << ref << *meta->origin() << std::endl;
        }
    }
    EXPECT_TRUE(reader->close());
}

TEST_F(parquet_readwrite_test, multi_row_groups) {
    boost::filesystem::path p{path()};
    p = p / "multi_row_groups.parquet";
    auto rec = mock::create_nullable_record<kind::int8, kind::float8>(10, 100.0);
    parquet_writer_option opt{};
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1"}
        ), p.string(), opt);
    ASSERT_TRUE(writer);

    writer->write(rec.ref());
    writer->write(rec.ref());
    writer->new_row_group();
    writer->write(rec.ref());
    writer->new_row_group();
    writer->write(rec.ref());
    writer->write(rec.ref());
    writer->close();
    EXPECT_EQ(p.string(), writer->path());
    EXPECT_EQ(5, writer->write_count());
    ASSERT_LT(0, boost::filesystem::file_size(p));

    {
        auto reader = parquet_reader::open(p.string());
        ASSERT_TRUE(reader);
        auto meta = reader->meta();
        ASSERT_EQ(2, meta->field_count());
        EXPECT_EQ("C0", meta->field_name(0));
        EXPECT_EQ("C1", meta->field_name(1));
        EXPECT_TRUE(meta->nullable(0));
        EXPECT_TRUE(meta->nullable(1));
        EXPECT_EQ(meta::field_type_kind::int8, meta->at(0).kind());
        EXPECT_EQ(meta::field_type_kind::float8, meta->at(1).kind());
        {
            accessor::record_ref ref{};
            ASSERT_TRUE(reader->next(ref));
            EXPECT_EQ(rec, mock::basic_record(ref, meta->origin()));
        }
        {
            accessor::record_ref ref{};
            ASSERT_TRUE(reader->next(ref));
            EXPECT_EQ(rec, mock::basic_record(ref, meta->origin()));
        }
        {
            accessor::record_ref ref{};
            ASSERT_FALSE(reader->next(ref));
        }
        EXPECT_EQ(2, reader->read_count());
        EXPECT_EQ(p.string(), reader->path());
        EXPECT_TRUE(reader->close());
    }
    {
        auto reader = parquet_reader::open(p.string(), nullptr, 1);
        ASSERT_TRUE(reader);
        auto meta = reader->meta();
        ASSERT_EQ(2, meta->field_count());
        EXPECT_EQ("C0", meta->field_name(0));
        EXPECT_EQ("C1", meta->field_name(1));
        EXPECT_TRUE(meta->nullable(0));
        EXPECT_TRUE(meta->nullable(1));
        EXPECT_EQ(meta::field_type_kind::int8, meta->at(0).kind());
        EXPECT_EQ(meta::field_type_kind::float8, meta->at(1).kind());
        {
            accessor::record_ref ref{};
            ASSERT_TRUE(reader->next(ref));
            EXPECT_EQ(rec, mock::basic_record(ref, meta->origin()));
        }
        {
            accessor::record_ref ref{};
            ASSERT_FALSE(reader->next(ref));
        }
        EXPECT_EQ(1, reader->read_count());
        EXPECT_EQ(p.string(), reader->path());
        EXPECT_TRUE(reader->close());
    }
    {
        auto reader = parquet_reader::open(p.string(), nullptr, 2);
        ASSERT_TRUE(reader);
        auto meta = reader->meta();
        ASSERT_EQ(2, meta->field_count());
        EXPECT_EQ("C0", meta->field_name(0));
        EXPECT_EQ("C1", meta->field_name(1));
        EXPECT_TRUE(meta->nullable(0));
        EXPECT_TRUE(meta->nullable(1));
        EXPECT_EQ(meta::field_type_kind::int8, meta->at(0).kind());
        EXPECT_EQ(meta::field_type_kind::float8, meta->at(1).kind());
        {
            accessor::record_ref ref{};
            ASSERT_TRUE(reader->next(ref));
            EXPECT_EQ(rec, mock::basic_record(ref, meta->origin()));
        }
        {
            accessor::record_ref ref{};
            ASSERT_TRUE(reader->next(ref));
            EXPECT_EQ(rec, mock::basic_record(ref, meta->origin()));
        }
        {
            accessor::record_ref ref{};
            ASSERT_FALSE(reader->next(ref));
        }
        EXPECT_EQ(2, reader->read_count());
        EXPECT_EQ(p.string(), reader->path());
        EXPECT_TRUE(reader->close());
    }
}

TEST_F(parquet_readwrite_test, char) {
    // verify writing char columns data as STRING
    boost::filesystem::path p{path()};
    p = p / "char.parquet";
    auto rec = mock::typed_nullable_record<kind::character, kind::character>(
        std::tuple{meta::character_type(false, 3), meta::character_type(false, 5)},
        std::forward_as_tuple(accessor::text("1  "), accessor::text("1    ")),
        {false, false}
    );

    parquet_writer_option opt{};
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1"}
        ),
        p.string(),
        opt
    );
    ASSERT_TRUE(writer);

    writer->write(rec.ref());
    writer->close();
    ASSERT_LT(0, boost::filesystem::file_size(p));

    auto reader = parquet_reader::open(p.string());
    ASSERT_TRUE(reader);
    auto meta = reader->meta();
    // originally the columns are char(n), but when reading they become varchar(*)
    // because both char/varchar are mapped to parquet type STRING
    ASSERT_EQ(2, meta->field_count());
    EXPECT_EQ(meta::field_type_kind::character, meta->at(0).kind());
    auto opt0 = meta->at(0).option<kind::character>();
    EXPECT_TRUE(opt0->varying_);
    EXPECT_FALSE(opt0->length_.has_value());
    EXPECT_EQ(meta::field_type_kind::character, meta->at(1).kind());
    auto opt1 = meta->at(1).option<kind::character>();
    EXPECT_TRUE(opt1->varying_);
    EXPECT_FALSE(opt1->length_.has_value());
    {
        accessor::record_ref ref{};
        ASSERT_TRUE(reader->next(ref));
        auto exp = mock::typed_nullable_record<kind::character, kind::character>(
            std::tuple{meta::character_type(true), meta::character_type(true)},
            std::forward_as_tuple(accessor::text("1  "), accessor::text("1    ")),
            {false, false}
        );
        EXPECT_EQ(exp, mock::basic_record(ref, meta->origin()));
    }
    EXPECT_TRUE(reader->close());
}

TEST_F(parquet_readwrite_test, fixed_len_binary) {
    // verify writing binary columns
    boost::filesystem::path p{path()};
    p = p / "fixed_binary.parquet";
    auto rec = mock::typed_nullable_record<kind::octet, kind::octet>(
        std::tuple{meta::octet_type(false, 3), meta::octet_type(false, 5)},
        std::forward_as_tuple(accessor::binary("\x01\x00\x00"), accessor::binary("\x01\x00\x00\x00\x00")),
        {false, false}
    );

    parquet_writer_option opt{};
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1"}
        ),
        p.string(),
        opt
    );
    ASSERT_TRUE(writer);

    writer->write(rec.ref());
    writer->close();
    ASSERT_LT(0, boost::filesystem::file_size(p));

    auto reader = parquet_reader::open(p.string());
    ASSERT_TRUE(reader);
    auto meta = reader->meta();
    // originally the columns are binary(n), but when reading they become varbinary(*)
    // because both binary/varbinary are mapped to parquet type BYTE_ARRAY with no length specified
    ASSERT_EQ(2, meta->field_count());
    EXPECT_EQ(meta::field_type_kind::octet, meta->at(0).kind());
    auto opt0 = meta->at(0).option<kind::octet>();
    EXPECT_TRUE(opt0->varying_);
    EXPECT_FALSE(opt0->length_.has_value());
    EXPECT_EQ(meta::field_type_kind::octet, meta->at(1).kind());
    auto opt1 = meta->at(1).option<kind::octet>();
    EXPECT_TRUE(opt1->varying_);
    EXPECT_FALSE(opt1->length_.has_value());
    {
        accessor::record_ref ref{};
        ASSERT_TRUE(reader->next(ref));
        // note: expected results are varbinary(*) instead of varbinary(n)
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::octet, kind::octet>(
                std::tuple{meta::octet_type(true), meta::octet_type(true)},
                std::forward_as_tuple(accessor::binary("\x01\x00\x00"), accessor::binary("\x01\x00\x00\x00\x00")),
                {false, false}
            )),
            mock::basic_record(ref, meta->origin())
        );
    }
    EXPECT_TRUE(reader->close());
}

TEST_F(parquet_readwrite_test, variable_len_binary) {
    // verify writing varbinary columns
    boost::filesystem::path p{path()};
    p = p / "varbinary.parquet";
    auto rec = mock::typed_nullable_record<kind::octet, kind::octet>(
        std::tuple{meta::octet_type(true, 3), meta::octet_type(true, 5)},
        std::forward_as_tuple(accessor::binary("\x01\x00\x00"), accessor::binary("\x01\x00\x00\x00\x00")),
        {false, false}
    );

    parquet_writer_option opt{};
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1"}
        ),
        p.string(),
        opt
    );
    ASSERT_TRUE(writer);
    writer->write(rec.ref());
    writer->close();
    ASSERT_LT(0, boost::filesystem::file_size(p));

    auto reader = parquet_reader::open(p.string());
    ASSERT_TRUE(reader);
    auto meta = reader->meta();
    // originally the columns are varbinary(n), but when reading they become varbinary(*)
    // because both binary/varbinary are mapped to parquet type BYTE_ARRAY with no length specified
    ASSERT_EQ(2, meta->field_count());
    EXPECT_EQ(meta::field_type_kind::octet, meta->at(0).kind());
    auto opt0 = meta->at(0).option<kind::octet>();
    EXPECT_TRUE(opt0->varying_);
    EXPECT_FALSE(opt0->length_.has_value());
    EXPECT_EQ(meta::field_type_kind::octet, meta->at(1).kind());
    auto opt1 = meta->at(1).option<kind::octet>();
    EXPECT_TRUE(opt1->varying_);
    EXPECT_FALSE(opt1->length_.has_value());
    {
        accessor::record_ref ref{};
        ASSERT_TRUE(reader->next(ref));
        // note: expected results are varbinary(*) instead of varbinary(n)
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::octet, kind::octet>(
                std::tuple{meta::octet_type(true), meta::octet_type(true)},
                std::forward_as_tuple(accessor::binary("\x01\x00\x00"), accessor::binary("\x01\x00\x00\x00\x00")),
                {false, false}
            )),
            mock::basic_record(ref, meta->origin())
        );
    }
    EXPECT_TRUE(reader->close());
}

}

