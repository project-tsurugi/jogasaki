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
#include <decimal.hh>
#include <exception>
#include <string_view>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>

#include <jogasaki/executor/expr/details/constants.h>
#include <jogasaki/executor/expr/details/decimal_context.h>


namespace jogasaki::testing {

using namespace std::string_view_literals;
using takatori::decimal::triple;
using namespace executor::expr::details;

// checking decimal library behavior, not actual test of jogasaki
class decimal_test : public ::testing::Test {};

TEST_F(decimal_test, build) {
    decimal::Context ctx{standard_decimal_context()};
    decimal::Decimal d{"3.14", ctx};
    decimal::Decimal exp{mpd_uint128_triple_t{MPD_TRIPLE_NORMAL, 0, 0, 314, -2} }; // sign=0 means positive
    EXPECT_EQ(exp, d);
}

TEST_F(decimal_test, individable) {
    decimal::context = executor::expr::details::standard_decimal_context();
    decimal::Decimal ten{triple{1,0,10,0}};
    decimal::Decimal three{triple{1,0,3,0}};
    auto result = ten/three;
    std::cerr << "result:" << result << std::endl;
    std::cerr << "digit:" << (result.coeff().adjexp()+1) << std::endl;
    auto t = result.as_uint128_triple();
    std::cerr << "tag:" << t.tag << std::endl;
    EXPECT_EQ((static_cast<triple>(decimal::Decimal{"3.3333333333333333333333333333333333333"})), static_cast<triple>(result));
}

TEST_F(decimal_test, from_triple) {
    auto zero = static_cast<::decimal::Decimal>(triple{0, 0, 0, 0});
    EXPECT_EQ(0, zero);

    auto ten0 = static_cast<::decimal::Decimal>(triple{1, 0, 10, 0});
    auto ten1 = static_cast<::decimal::Decimal>(triple{1, 0, 1, 1});
    EXPECT_EQ(10, ten0);
    EXPECT_EQ(10, ten1);
    EXPECT_EQ(ten0, ten1);

    auto nten0 = static_cast<::decimal::Decimal>(triple{-1, 0, 10, 0});
    auto nten1 = static_cast<::decimal::Decimal>(triple{-1, 0, 1, 1});
    EXPECT_EQ(-10, nten0);
    EXPECT_EQ(-10, nten1);
    EXPECT_EQ(nten0, nten1);
}

TEST_F(decimal_test, from_triple_min_max) {
    decimal::context = standard_decimal_context();
    {
        auto tri = triple{1, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0};
        auto maxDec = static_cast<::decimal::Decimal>(tri);
        EXPECT_FALSE(decimal::context.status());
        EXPECT_EQ("340282366920938463463374607431768211455", maxDec.to_sci());
        auto back = static_cast<triple>(maxDec);
        EXPECT_FALSE(decimal::context.status());
        EXPECT_EQ(tri, back);
    }
    {
        auto tri = triple{-1, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0};
        auto minDec = static_cast<::decimal::Decimal>(tri);
        EXPECT_FALSE(decimal::context.status());
        EXPECT_EQ("-340282366920938463463374607431768211455", minDec.to_sci());
        auto back = static_cast<triple>(minDec);
        EXPECT_FALSE(decimal::context.status());
        EXPECT_EQ(tri, back);
    }
}

TEST_F(decimal_test, triple_zero) {
    triple zero{0, 0, 0, 0};
    triple positive_zero{1, 0, 0, 0};
    triple negative_zero{-1, 0, 0, 0};
    EXPECT_EQ(zero, positive_zero);
    EXPECT_EQ(zero, negative_zero);
}

TEST_F(decimal_test, decimal128) {
    // see what is the decimal128 format
    decimal::context = decimal::IEEEContext(128);
    EXPECT_EQ(6144, decimal::context.emax());
    EXPECT_EQ(-6143, decimal::context.emin());
    EXPECT_EQ(-6176, decimal::context.etiny());
    EXPECT_EQ(34, decimal::context.prec());
    EXPECT_EQ(0, decimal::context.traps());
    EXPECT_EQ(MPD_ROUND_HALF_EVEN, decimal::context.round());
}

TEST_F(decimal_test, decimal160) {
    // next candidate context larger than decimal128
    decimal::context = decimal::IEEEContext(128+32);
    EXPECT_EQ(24576, decimal::context.emax());
    EXPECT_EQ(-24575, decimal::context.emin());
    EXPECT_EQ(-24617, decimal::context.etiny());
    EXPECT_EQ(43, decimal::context.prec());
    EXPECT_EQ(0, decimal::context.traps());
    EXPECT_EQ(MPD_ROUND_HALF_EVEN, decimal::context.round());
}

TEST_F(decimal_test, default_context) {
    // see what is the default context
    decimal::context = decimal::Context{};
    EXPECT_EQ(999999, decimal::context.emax());
    EXPECT_EQ(-999999, decimal::context.emin());
    EXPECT_EQ(-1000014, decimal::context.etiny());
    EXPECT_EQ(16, decimal::context.prec());
    EXPECT_EQ(MPD_IEEE_Invalid_operation | MPD_Division_by_zero | MPD_Overflow, decimal::context.traps());
    EXPECT_EQ(MPD_ROUND_HALF_EVEN, decimal::context.round());
}

TEST_F(decimal_test, max_context) {
    decimal::context = decimal::MaxContext();
    EXPECT_EQ(999999999999999999, decimal::context.emax());
    EXPECT_EQ(-999999999999999999, decimal::context.emin());
    EXPECT_EQ(-1999999999999999997, decimal::context.etiny());
    EXPECT_EQ(999999999999999999, decimal::context.prec());
    EXPECT_EQ(MPD_IEEE_Invalid_operation, decimal::context.traps());
    EXPECT_EQ(MPD_ROUND_HALF_EVEN, decimal::context.round());
}

TEST_F(decimal_test, standard_context) {
    decimal::context = standard_decimal_context();
    EXPECT_EQ(24576, decimal::context.emax());
    EXPECT_EQ(-24575, decimal::context.emin());
    EXPECT_EQ(-24612, decimal::context.etiny());
    EXPECT_EQ(38, decimal::context.prec());
    EXPECT_EQ(0, decimal::context.traps());
    EXPECT_EQ(MPD_ROUND_HALF_EVEN, decimal::context.round());
}

TEST_F(decimal_test, calculation_in_max_context) {
    // verify large number calculation is correct in MaxContext, while not in IEEEContext(128)
    {
        decimal::context = decimal::MaxContext();
        decimal::Decimal dec0{triple{1, 0, 2, 0}};
        decimal::Decimal dec1{triple{1, 0x7FFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0}};
        auto dec2 = dec0 * dec1;
        EXPECT_FALSE(decimal::context.status() & MPD_Inexact);
        decimal::Decimal exp{triple{1, 0xFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFE, 0}};
        EXPECT_EQ(exp, dec2);
    }
    {
        decimal::context = decimal::IEEEContext(128);
        decimal::Decimal dec0{triple{1, 0, 2, 0}};
        decimal::Decimal dec1{triple{1, 0x7FFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0}};
        auto dec2 = dec0 * dec1;
        EXPECT_TRUE(decimal::context.status() & MPD_Inexact);
        decimal::Decimal exp{triple{1, 0xFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFE, 0}};
        EXPECT_NE(exp, dec2);
    }
}

// verify behavior of large number around max digits for decimal

TEST_F(decimal_test, value_max_decimal_from_string) {
    // verify behavior of max digits of decimal
    decimal::context = standard_decimal_context();
    decimal::Decimal d{"99999999999999999999999999999999999999"}; // 38 digits
    std::cerr << d << std::endl;
    EXPECT_EQ(0, decimal::context.status());
    {
        // generation
        decimal::context.clear_status();
        auto tri = static_cast<triple>(d);
        EXPECT_EQ(0, decimal::context.status());
        EXPECT_EQ(triple_max_of_decimal_38_0, tri);
    }
    {
        // check reduce success
        decimal::context.clear_status();
        auto dd = d.reduce();
        EXPECT_EQ(0, decimal::context.status());
        auto tri = static_cast<triple>(dd);
        decimal::Decimal exp{"99999999999999999999999999999999999999"};
        EXPECT_EQ(static_cast<triple>(exp), tri);
    }
    {
        // convert to triple
        decimal::context.clear_status();
        auto t = d.as_uint128_triple();
        EXPECT_EQ(MPD_TRIPLE_NORMAL, t.tag);
        EXPECT_EQ(0, decimal::context.status());
        auto tri = static_cast<triple>(d);
        decimal::Decimal exp{"99999999999999999999999999999999999999"};
        EXPECT_EQ(triple_max_of_decimal_38_0, tri);
    }
}

TEST_F(decimal_test, value_max_decimal_plus_one_from_string) {
    // verify behavior of seemingly exceeding max digits of decimal, not actually exceeding
    decimal::context = standard_decimal_context();
    decimal::Decimal d{"100000000000000000000000000000000000000"}; // 39 digits
    std::cerr << d << std::endl;
    EXPECT_EQ(0, decimal::context.status());
    {
        // generation itself is successful
        decimal::context.clear_status();
        auto tri = static_cast<triple>(d);
        EXPECT_EQ(0, decimal::context.status());
        EXPECT_EQ(triple_max_of_decimal_38_0_plus_one, tri);
    }
    {
        // check reduce behavior
        decimal::context.clear_status();
        auto dd = d.reduce();
        EXPECT_EQ(MPD_Rounded, decimal::context.status()); // not inexact because the result is power of ten
        auto tri = static_cast<triple>(dd);
        decimal::Decimal exp{"1E38"};
        EXPECT_EQ(static_cast<triple>(exp), tri);
    }
    {
        // convert to triple - this case won't be the case with "outside of triple" because the result has trailing zero
        decimal::context.clear_status();
        auto t = d.as_uint128_triple();
        EXPECT_EQ(MPD_TRIPLE_NORMAL, t.tag);
        EXPECT_EQ(0, decimal::context.status());
        EXPECT_NO_THROW({auto tri = static_cast<triple>(d); });
    }
}

TEST_F(decimal_test, value_max_decimal_plus_two_from_string) {
    // verify behavior of exceeding max digits of decimal, requiring rounding
    decimal::context = standard_decimal_context();
    decimal::Decimal d{"100000000000000000000000000000000000001"}; // 39 digits
    std::cerr << d << std::endl;
    EXPECT_EQ(0, decimal::context.status());
    {
        // generation itself is successful
        decimal::context.clear_status();
        auto tri = static_cast<triple>(d);
        EXPECT_EQ(0, decimal::context.status());
        EXPECT_EQ(triple_max_of_decimal_38_0_plus_two, tri);
    }
    {
        // check reduce behavior
        decimal::context.clear_status();
        auto dd = d.reduce();
        EXPECT_EQ(MPD_Inexact | MPD_Rounded, decimal::context.status());  // inexactly rounded
        auto tri = static_cast<triple>(dd);
        decimal::Decimal exp{"1E38"};
        EXPECT_EQ(static_cast<triple>(exp), tri);
    }
    {
        // convert to triple - this case is an error
        decimal::context.clear_status();
        auto t = d.as_uint128_triple();
        EXPECT_EQ(MPD_TRIPLE_NORMAL, t.tag);
        EXPECT_EQ(0, decimal::context.status());
        triple tri{};
        EXPECT_NO_THROW({tri = static_cast<triple>(d); });
        EXPECT_EQ(triple_max_of_decimal_38_0_plus_two, tri);

        // to detect coefficient is out of range, we need to check the coefficient itself
        EXPECT_LT(38, d.coeff().adjexp()+1);
    }
}

TEST_F(decimal_test, value_max_triple_from_string) {
    // verify max coefficient in triple
    decimal::context = standard_decimal_context();
    decimal::Decimal d{"340282366920938463463374607431768211455"}; // 2^128 - 1
    std::cerr << d << std::endl;
    EXPECT_EQ(0, decimal::context.status());
    {
        // generation itself is successful
        decimal::context.clear_status();
        auto tri = static_cast<triple>(d);
        EXPECT_EQ(0, decimal::context.status());
        EXPECT_EQ((triple{1, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0}), tri);
    }
    {
        // check reduce behavior
        decimal::context.clear_status();
        auto dd = d.reduce();
        EXPECT_EQ(MPD_Inexact | MPD_Rounded, decimal::context.status());
        auto tri = static_cast<triple>(dd);
        decimal::Decimal exp{"34028236692093846346337460743176821146E1"}; // rounded and result exp = 1
        EXPECT_EQ(static_cast<triple>(exp), tri);
    }
    {
        // check larger prec makes exact handling on the reduce behavior
        decimal::context.clear_status();
        decimal::context.prec(39);
        auto tri = static_cast<triple>(d.reduce());
        EXPECT_EQ(0, decimal::context.status());
        EXPECT_EQ((triple{1, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0}), tri);
        decimal::context.prec(38);
    }
}

TEST_F(decimal_test, value_outside_triple_from_string) {
    // verify generating large number outside triple raises exception when converting to triple
    decimal::context = standard_decimal_context();
    decimal::Decimal d{"340282366920938463463374607431768211456"}; // 2^128
    std::cerr << d << std::endl;
    EXPECT_EQ(0, decimal::context.status());
    EXPECT_EQ(MPD_TRIPLE_ERROR, d.as_uint128_triple().tag);
    EXPECT_EQ(0, decimal::context.status());
    EXPECT_THROW({static_cast<triple>(d); }, std::invalid_argument);
}

TEST_F(decimal_test, subnormal) {
    decimal::context = decimal::IEEEContext(128);
    // {
    //     triple norm{1, 0, 1, -6175L};
    //     decimal::Decimal dec{static_cast<mpd_uint128_triple_t>(norm)};
    //     EXPECT_FALSE(dec.issubnormal());
    //     EXPECT_EQ(-6175, dec.adjexp());
    // }
    {
        triple subnorm{1, 0, 1, -6177L};
        decimal::Decimal dec{static_cast<mpd_uint128_triple_t>(subnorm)};
        EXPECT_TRUE(dec.issubnormal());
        EXPECT_EQ(-6177, dec.adjexp());
    }

    decimal::context = decimal::IEEEContext(128);
    decimal::context.clear_status();
    triple sn{1, 0, 1, -6177L};
    decimal::Decimal dec{static_cast<mpd_uint128_triple_t>(sn)};
    // TODO investigate why status flag is not set
    // EXPECT_TRUE(decimal::context.status() & MPD_Subnormal);
    std::cerr << "dec: " << dec.to_sci() << std::endl;
    EXPECT_TRUE(dec.issubnormal());
}

TEST_F(decimal_test, outside_triple) {
    decimal::context = decimal::MaxContext();
    {
        auto tri = triple{1, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0};
        auto maxDec = static_cast<::decimal::Decimal>(tri);
        EXPECT_FALSE(decimal::context.status());
        auto plus1 = maxDec + 1;
        EXPECT_FALSE(decimal::context.status());
        EXPECT_EQ("340282366920938463463374607431768211456", plus1.to_sci());
        auto t = plus1.as_uint128_triple();
        EXPECT_EQ(MPD_TRIPLE_ERROR, t.tag);
        bool caught = false;
        try {
            auto back = static_cast<triple>(plus1);
        } catch (std::exception const& e) {
            std::cerr << "exception: " << e.what() << std::endl;
            caught = true;
        }
        ASSERT_TRUE(caught);
    }
    {
        auto tri = triple{-1, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0};
        auto minDec = static_cast<::decimal::Decimal>(tri);
        EXPECT_FALSE(decimal::context.status());
        auto minus1 = minDec - 1;
        EXPECT_FALSE(decimal::context.status());
        EXPECT_EQ("-340282366920938463463374607431768211456", minus1.to_sci());
        auto t = minus1.as_uint128_triple();
        EXPECT_EQ(MPD_TRIPLE_ERROR, t.tag);
        bool caught = false;
        try {
            auto back = static_cast<triple>(minus1);
        } catch (std::exception const& e) {
            std::cerr << "exception: " << e.what() << std::endl;
            caught = true;
        }
        ASSERT_TRUE(caught);
    }
}

TEST_F(decimal_test, rescale) {
    // see rescale behavior
    decimal::context = standard_decimal_context();
    auto tri = triple{1, 0, 314, -2};
    auto dec = static_cast<::decimal::Decimal>(tri);
    EXPECT_EQ("3.14", dec.to_sci());

    {
        decimal::context.clear_status();
        auto rescaled = dec.rescale(0);
        EXPECT_TRUE(decimal::context.status() & MPD_Inexact);
        EXPECT_NE(dec, rescaled);
        EXPECT_EQ("3", rescaled.to_sci());
    }
    {
        decimal::context.clear_status();
        auto rescaled = dec.rescale(-1);
        EXPECT_TRUE(decimal::context.status() & MPD_Inexact);
        EXPECT_NE(dec, rescaled);
        EXPECT_EQ("3.1", rescaled.to_sci());
    }
    {
        decimal::context.clear_status();
        auto rescaled = dec.rescale(-2);
        EXPECT_FALSE(decimal::context.status() & MPD_Inexact);
        EXPECT_EQ(dec, rescaled);
        EXPECT_EQ("3.14", rescaled.to_sci());
    }
    {
        decimal::context.clear_status();
        auto rescaled = dec.rescale(-3);
        EXPECT_FALSE(decimal::context.status() & MPD_Inexact);
        EXPECT_EQ(dec, rescaled);
        EXPECT_EQ("3.140", rescaled.to_sci());
    }
    {
        decimal::context.clear_status();
        auto rescaled = dec.rescale(1);
        EXPECT_TRUE(decimal::context.status() & MPD_Inexact);
        EXPECT_NE(dec, rescaled);
        EXPECT_EQ("0E+1", rescaled.to_sci());
    }
    {
        decimal::context.clear_status();
        auto rescaled = dec.rescale(2);
        EXPECT_TRUE(decimal::context.status() & MPD_Inexact);
        EXPECT_NE(dec, rescaled);
        EXPECT_EQ("0E+2", rescaled.to_sci());
    }
}

TEST_F(decimal_test, rescale_large_value) {
    // see rescale behavior with large value
    decimal::context = standard_decimal_context();
    auto tri = triple{1, 0, 314, -2};
    auto dec = ::decimal::Decimal{"123456789012345678901234567890123456.78"};
    EXPECT_EQ("123456789012345678901234567890123456.78", dec.to_sci());
    {
        decimal::context.clear_status();
        decimal::context.round(MPD_ROUND_DOWN);
        auto rescaled = dec.rescale(-1);
        EXPECT_TRUE(decimal::context.status() & MPD_Inexact);
        EXPECT_NE(dec, rescaled);
        EXPECT_EQ("123456789012345678901234567890123456.7", rescaled.to_sci());
    }
}

TEST_F(decimal_test, rescale_large_value2) {
    // see rescale behavior with large value
    decimal::context = standard_decimal_context();
    auto dec = ::decimal::Decimal{"999999999999999999999999999999999999.99"};
    EXPECT_EQ("999999999999999999999999999999999999.99", dec.to_sci());
    {
        decimal::context.clear_status();
        decimal::context.round(MPD_ROUND_DOWN);
        auto rescaled = dec.rescale(-1);
        EXPECT_TRUE(decimal::context.status() & MPD_Inexact);
        EXPECT_NE(dec, rescaled);
        EXPECT_EQ("999999999999999999999999999999999999.9", rescaled.to_sci());
        EXPECT_EQ(-1, rescaled.exponent());
        EXPECT_EQ("999999999999999999999999999999999999.9", rescaled.reduce().to_sci());
        EXPECT_EQ(-1, rescaled.reduce().exponent());
    }
}

TEST_F(decimal_test, rescale_large_value3) {
    // see rescale behavior with large value
    decimal::context = standard_decimal_context();
    auto dec = ::decimal::Decimal{"340282366920938463463374607431768211455"};
    EXPECT_EQ("340282366920938463463374607431768211455", dec.to_sci());
    {
        decimal::context.clear_status();
        decimal::context.round(MPD_ROUND_DOWN);
        auto rescaled = dec.rescale(1);
        EXPECT_TRUE(decimal::context.status() & MPD_Inexact);
        EXPECT_NE(dec, rescaled);
        EXPECT_EQ("3.4028236692093846346337460743176821145E+38", rescaled.to_sci());
        EXPECT_EQ(1, rescaled.exponent());
        EXPECT_EQ("3.4028236692093846346337460743176821145E+38", rescaled.reduce().to_sci());
        EXPECT_EQ(1, rescaled.reduce().exponent());
    }
}

TEST_F(decimal_test, rescale_reduce_large_exponents) {
    // see rescale behavior with values whose exponent is large
    decimal::context = standard_decimal_context();
    auto dec = ::decimal::Decimal{"1.11E+2147483646"};  //  arbitrarily large exponent
    EXPECT_EQ("1.11E+2147483646", dec.to_sci());
    {
        decimal::context.clear_status();
        auto round = decimal::context.round();
        decimal::context.round(MPD_ROUND_DOWN);
        auto exp = dec.exponent();
        auto rescaled = dec.rescale(exp+1);
        decimal::context.round(round);
        EXPECT_TRUE(decimal::context.status() & MPD_Inexact);
        EXPECT_NE(dec, rescaled);
        EXPECT_EQ("1.1E+2147483646", rescaled.to_sci()); // rescale worked even adjusted exponent is larger than emax (probably because coefficient digit is small?)
        EXPECT_EQ(2147483645, rescaled.exponent());

        // reduced is affected by the emax setting of context
        decimal::context.clear_status();
        EXPECT_EQ("Infinity", rescaled.reduce().to_sci());  // reduce makes it infinity
        EXPECT_EQ(MPD_Inexact | MPD_Overflow | MPD_Rounded, decimal::context.status());
    }
}

TEST_F(decimal_test, rescale_reduce_max_exponents) {
    // see rescale behavior with values whose exponent is max
    decimal::context = standard_decimal_context();
    auto dec = ::decimal::Decimal{"9.99999999999999999999999999999999999999E+24576"};  // 39 digits with emax
    EXPECT_EQ("9.99999999999999999999999999999999999999E+24576", dec.to_sci());
    {
        decimal::context.clear_status();
        auto round = decimal::context.round();
        decimal::context.round(MPD_ROUND_DOWN);
        auto exp = dec.exponent();
        EXPECT_EQ(24538, exp);
        auto rescaled = dec.rescale(exp+1);
        decimal::context.round(round);
        EXPECT_EQ(MPD_Inexact | MPD_Rounded, decimal::context.status());
        EXPECT_NE(dec, rescaled);
        EXPECT_EQ("9.9999999999999999999999999999999999999E+24576", rescaled.to_sci()); // digit truncted, rescale worked
        EXPECT_EQ(24539, rescaled.exponent());

        // reduced is ok too
        decimal::context.clear_status();
        EXPECT_EQ("9.9999999999999999999999999999999999999E+24576", rescaled.reduce().to_sci());
        EXPECT_EQ(0, decimal::context.status());
        EXPECT_EQ(24539, rescaled.reduce().exponent());
    }
}

TEST_F(decimal_test, rescale_reduce_max_exponents_minus_one) {
    // see rescale behavior with values whose exponent is max-1
    decimal::context = standard_decimal_context();
    auto dec = ::decimal::Decimal{"9.99999999999999999999999999999999999999E+24575"};  // 39 digits with emax - 1
    EXPECT_EQ("9.99999999999999999999999999999999999999E+24575", dec.to_sci());
    {
        decimal::context.clear_status();
        auto round = decimal::context.round();
        decimal::context.round(MPD_ROUND_DOWN);
        auto exp = dec.exponent();
        EXPECT_EQ(24537, exp);
        auto rescaled = dec.rescale(exp+1);
        decimal::context.round(round);
        EXPECT_EQ(MPD_Inexact | MPD_Rounded, decimal::context.status());
        EXPECT_NE(dec, rescaled);
        EXPECT_EQ("9.9999999999999999999999999999999999999E+24575", rescaled.to_sci()); // rescale didn't work
        EXPECT_EQ(24538, rescaled.exponent());

        // reduced is affected by the emax setting of context
        decimal::context.clear_status();
        EXPECT_EQ("9.9999999999999999999999999999999999999E+24575", rescaled.reduce().to_sci());
        EXPECT_EQ(0, decimal::context.status());
        EXPECT_EQ(24538, rescaled.reduce().exponent());
    }
}

TEST_F(decimal_test, reduce) {
    // see reduce behavior
    decimal::context = standard_decimal_context();
    {
        auto tri = triple{1, 0, 314, -2};
        auto dec = static_cast<::decimal::Decimal>(tri);
        EXPECT_EQ("3.14", dec.to_sci());
        decimal::context.clear_status();
        auto reduced = dec.reduce();
        EXPECT_FALSE(decimal::context.status() & MPD_Inexact);
        EXPECT_EQ(dec, reduced);
        EXPECT_EQ("3.14", reduced.to_sci());
        auto back = static_cast<triple>(reduced);
        EXPECT_EQ(tri, back);
    }
    {
        auto tri = triple{1, 0, 3140, -3};
        auto dec = static_cast<::decimal::Decimal>(tri);
        EXPECT_EQ("3.140", dec.to_sci());
        decimal::context.clear_status();
        auto reduced = dec.reduce();
        EXPECT_FALSE(decimal::context.status() & MPD_Inexact);
        EXPECT_EQ(dec, reduced);
        EXPECT_EQ("3.14", reduced.to_sci());
        auto back = static_cast<triple>(reduced);
        EXPECT_EQ((triple{1, 0, 314, -2}), back);
    }
    {
        auto tri = triple{1, 0, 3140, 0};
        auto dec = static_cast<::decimal::Decimal>(tri);
        EXPECT_EQ("3140", dec.to_sci());
        decimal::context.clear_status();
        auto reduced = dec.reduce();
        EXPECT_FALSE(decimal::context.status() & MPD_Inexact);
        EXPECT_EQ(dec, reduced);
        EXPECT_EQ("3.14E+3", reduced.to_sci());  // exp > 0 makes the representation with exponential
        auto back = static_cast<triple>(reduced);
        EXPECT_EQ((triple{1, 0, 314, 1}), back);
    }
}

TEST_F(decimal_test, triple_round_trip) {
    // see round trip behavior of triple -> Decimal -> triple
    decimal::context = standard_decimal_context();
    {
        auto tri = triple{1, 0, 314, -2};
        auto dec = static_cast<::decimal::Decimal>(tri);
        EXPECT_EQ("3.14", dec.to_sci());
        decimal::context.clear_status();
        auto back = static_cast<triple>(dec);
        EXPECT_EQ(tri, back);
    }
    {
        auto tri = triple{1, 0, 3140, -3};
        auto dec = static_cast<::decimal::Decimal>(tri);
        EXPECT_EQ("3.140", dec.to_sci());
        decimal::context.clear_status();
        auto back = static_cast<triple>(dec);
        EXPECT_EQ(tri, back);
        EXPECT_NE((triple{1, 0, 314, -2}), back);
    }
}

TEST_F(decimal_test, string_representation_special_values_mpdecimal) {
    decimal::context = standard_decimal_context();
    {
        ::decimal::Decimal d{"NaN"};
        decimal::context.clear_status();
        EXPECT_EQ("NaN", d.to_sci());
    }
    {
        ::decimal::Decimal d{"-NaN"};
        decimal::context.clear_status();
        EXPECT_EQ("-NaN", d.to_sci());  // mpdecimal puts -NaN
    }
    {
        ::decimal::Decimal d{"Inf"};
        decimal::context.clear_status();
        EXPECT_EQ("Infinity", d.to_sci());
    }
    {
        ::decimal::Decimal d{"Infinity"};
        decimal::context.clear_status();
        EXPECT_EQ("Infinity", d.to_sci());
    }
    {
        ::decimal::Decimal d{"-Inf"};
        decimal::context.clear_status();
        EXPECT_EQ("-Infinity", d.to_sci());
    }
    {
        ::decimal::Decimal d{"-Infinity"};
        decimal::context.clear_status();
        EXPECT_EQ("-Infinity", d.to_sci());
    }
}

TEST_F(decimal_test, string_representation_special_values_stdlib) {
    // std::to_string shows all special values in short lowercase
    decimal::context = standard_decimal_context();
    {
        auto d = std::numeric_limits<double>::quiet_NaN();
        EXPECT_EQ("nan", std::to_string(d));
    }
    {
        auto d = -std::numeric_limits<double>::quiet_NaN();
        EXPECT_EQ("-nan", std::to_string(d)); // to_string shows -nan
    }
    {
        auto d = std::numeric_limits<double>::infinity();
        EXPECT_EQ("inf", std::to_string(d));
    }
    {
        auto d = -std::numeric_limits<double>::infinity();
        EXPECT_EQ("-inf", std::to_string(d));
    }
}

}  // namespace jogasaki::testing
