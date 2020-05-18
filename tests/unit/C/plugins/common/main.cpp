#include <gtest/gtest.h>
#include <resultset.h>
#include <string.h>
#include <string>

/*
 * Fledge Readings to OMF translation unit tests
 *
 * Copyright (c) 2018 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Massimiliano Pinto
 */


using namespace std;

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);

    testing::GTEST_FLAG(repeat) = 1;
    testing::GTEST_FLAG(shuffle) = true;

    return RUN_ALL_TESTS();
}

