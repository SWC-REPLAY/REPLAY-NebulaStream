/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <TaskContext.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <QueryId.hpp>

namespace NES
{

/// The round trips fail if QueryId's operator<< changes its format without parseTaskContext following.
TEST(TaskContextTest, DistributedQueryWithPipelineRoundTrips)
{
    const auto localQueryId = LocalQueryId(generateUUID());
    const auto parsed
        = parseTaskContext(formatTaskContext(QueryId::create(localQueryId, DistributedQueryId("swift_appaloosa_0421")), PipelineId(12)));
    EXPECT_EQ(parsed.localQueryId, localQueryId.getRawValue());
    EXPECT_EQ(parsed.distributedQueryId, "swift_appaloosa_0421");
    EXPECT_EQ(parsed.pipelineId, PipelineId(12));
}

TEST(TaskContextTest, LocalQueryWithoutPipelineRoundTrips)
{
    const auto localQueryId = LocalQueryId(generateUUID());
    const auto parsed = parseTaskContext(formatTaskContext(QueryId::createLocal(localQueryId)));
    EXPECT_EQ(parsed.localQueryId, localQueryId.getRawValue());
    EXPECT_FALSE(parsed.distributedQueryId.has_value());
    EXPECT_FALSE(parsed.pipelineId.has_value());
}

TEST(TaskContextTest, DistributedIdMayContainSeparators)
{
    const auto parsed = parseTaskContext(formatTaskContext(QueryId::createDistributed(DistributedQueryId("a, b)-c:3")), PipelineId(4)));
    EXPECT_FALSE(parsed.localQueryId.has_value());
    EXPECT_EQ(parsed.distributedQueryId, "a, b)-c:3");
    EXPECT_EQ(parsed.pipelineId, PipelineId(4));
}

TEST(TaskContextTest, OtherTextHasNoParts)
{
    for (const auto* text : {"", "query-1", "QueryId(INVALID)", "QueryId(local=x)-notanumber"})
    {
        const auto parsed = parseTaskContext(text);
        EXPECT_FALSE(parsed.distributedQueryId.has_value()) << text;
        EXPECT_FALSE(parsed.pipelineId.has_value()) << text;
    }
}

}
