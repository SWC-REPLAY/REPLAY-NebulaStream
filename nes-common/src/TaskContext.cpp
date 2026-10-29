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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <Identifiers/Identifiers.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <QueryId.hpp>

namespace NES
{

std::string formatTaskContext(const QueryId& queryId)
{
    return fmt::format("{}", queryId);
}

std::string formatTaskContext(const QueryId& queryId, const PipelineId pipelineId)
{
    return fmt::format("{}-{}", queryId, pipelineId);
}

/// Follows QueryId's operator<<: `QueryId(local=<uuid>, distributed=<name>)`, `QueryId(local=<uuid>)` or
/// `QueryId(distributed=<name>)`. A distributed name may contain any character, so it ends at the last ')'.
ParsedTaskContext parseTaskContext(const std::string_view value)
{
    static constexpr std::string_view Prefix = "QueryId(";
    static constexpr std::string_view LocalKey = "local=";
    static constexpr std::string_view DistributedKey = "distributed=";

    ParsedTaskContext parsed;
    const auto close = value.rfind(')');
    if (not value.starts_with(Prefix) or close == std::string_view::npos)
    {
        return parsed;
    }
    auto inner = value.substr(Prefix.size(), close - Prefix.size());

    if (inner.starts_with(LocalKey))
    {
        const auto end = inner.find(", ");
        /// A distributed QueryId without a local part prints the invalid UUID as its local id.
        if (const auto localQueryId = inner.substr(LocalKey.size(), end - LocalKey.size());
            localQueryId != INVALID_LOCAL_QUERY_ID.getRawValue())
        {
            parsed.localQueryId = std::string(localQueryId);
        }
        inner = end == std::string_view::npos ? std::string_view{} : inner.substr(end + 2);
    }
    if (inner.starts_with(DistributedKey))
    {
        parsed.distributedQueryId = std::string(inner.substr(DistributedKey.size()));
    }

    if (const auto suffix = value.substr(close + 1); suffix.starts_with('-'))
    {
        if (const auto pipelineId = from_chars<uint64_t>(suffix.substr(1)))
        {
            parsed.pipelineId = PipelineId(*pipelineId);
        }
    }
    return parsed;
}

}
