// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "service/service_be/arrow_flight_sql_service.h"

#include <arrow/util/compression.h>
#include <gtest/gtest.h>

namespace starrocks {

TEST(ArrowFlightSqlServiceTest, ResolveCompressionCodec_None) {
    ASSERT_EQ(ArrowFlightSqlServer::resolve_compression_codec("none"), arrow::Compression::UNCOMPRESSED);
}

TEST(ArrowFlightSqlServiceTest, ResolveCompressionCodec_Empty) {
    ASSERT_EQ(ArrowFlightSqlServer::resolve_compression_codec(""), arrow::Compression::UNCOMPRESSED);
}

TEST(ArrowFlightSqlServiceTest, ResolveCompressionCodec_Lz4) {
    ASSERT_EQ(ArrowFlightSqlServer::resolve_compression_codec("lz4"), arrow::Compression::LZ4_FRAME);
}

TEST(ArrowFlightSqlServiceTest, ResolveCompressionCodec_Zstd) {
    ASSERT_EQ(ArrowFlightSqlServer::resolve_compression_codec("zstd"), arrow::Compression::ZSTD);
}

TEST(ArrowFlightSqlServiceTest, ResolveCompressionCodec_Unknown) {
    ASSERT_EQ(ArrowFlightSqlServer::resolve_compression_codec("snappy"), arrow::Compression::UNCOMPRESSED);
}

// Header override: header value wins over config when non-empty.
TEST(ArrowFlightSqlServiceTest, ResolveCompressionCodec_HeaderOverridesConfig) {
    ASSERT_EQ(ArrowFlightSqlServer::resolve_compression_codec("none", "lz4"), arrow::Compression::LZ4_FRAME);
}

TEST(ArrowFlightSqlServiceTest, ResolveCompressionCodec_HeaderOverridesConfig_Zstd) {
    ASSERT_EQ(ArrowFlightSqlServer::resolve_compression_codec("lz4", "zstd"), arrow::Compression::ZSTD);
}

TEST(ArrowFlightSqlServiceTest, ResolveCompressionCodec_EmptyHeaderFallsBackToConfig) {
    ASSERT_EQ(ArrowFlightSqlServer::resolve_compression_codec("lz4", ""), arrow::Compression::LZ4_FRAME);
}

TEST(ArrowFlightSqlServiceTest, ResolveCompressionCodec_BothEmptyIsUncompressed) {
    ASSERT_EQ(ArrowFlightSqlServer::resolve_compression_codec("none", ""), arrow::Compression::UNCOMPRESSED);
}

} // namespace starrocks
