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

#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <Identifiers/Identifiers.hpp>
#include <QueryId.hpp>

namespace NES
{

/// The value of the `Task` LogContext of a query engine task: `<queryId>` or `<queryId>-<pipelineId>`, with the QueryId as it
/// prints itself. Kept in one place, because the ErrorLog parses it back (parseTaskContext).
std::string formatTaskContext(const QueryId& queryId);
std::string formatTaskContext(const QueryId& queryId, PipelineId pipelineId);

struct ParsedTaskContext
{
    std::optional<std::string> localQueryId;
    std::optional<std::string> distributedQueryId;
    std::optional<PipelineId> pipelineId;
};

/// Inverse of formatTaskContext; also accepts a QueryId alone (the `queryId` LogContext). Parts it cannot find stay empty.
ParsedTaskContext parseTaskContext(std::string_view value);

}
