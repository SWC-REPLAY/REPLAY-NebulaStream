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

#include <ErrorSource.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <ranges>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <ErrorLog.hpp>
#include <InputFormatterDescriptor.hpp>
#include <Thread.hpp>

namespace NES
{

namespace
{
/// The CSV formatter splits tuples on '\n' regardless of quotes and keeps quotes in values, so this only keeps one record
/// on one line with balanced quotes; the values arrive escaped.
std::string escapeCsv(const std::string_view value)
{
    std::string escaped;
    escaped.reserve(value.size());
    for (const char character : value)
    {
        if (character == '\n')
        {
            escaped += "\\n";
        }
        else if (character == '"')
        {
            escaped += "\"\"";
        }
        else
        {
            escaped += character;
        }
    }
    return escaped;
}

/// JSON forbids raw control characters (below 0x20) in strings.
constexpr unsigned char FIRST_PRINTABLE_CHARACTER = 0x20;

std::string escapeJson(const std::string_view value)
{
    std::string escaped;
    escaped.reserve(value.size());
    for (const char character : value)
    {
        switch (character)
        {
            case '"':
                escaped += "\\\"";
                break;
            case '\\':
                escaped += "\\\\";
                break;
            case '\n':
                escaped += "\\n";
                break;
            case '\r':
                escaped += "\\r";
                break;
            case '\t':
                escaped += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(character) < FIRST_PRINTABLE_CHARACTER)
                {
                    escaped += fmt::format("\\u{:04x}", static_cast<unsigned char>(character));
                }
                else
                {
                    escaped += character;
                }
        }
    }
    return escaped;
}

uint64_t toUnixMilliseconds(const std::chrono::system_clock::time_point timestamp)
{
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(timestamp.time_since_epoch()).count());
}

/// ctx uses the `key:value key:value` form of the `[%&]` log pattern field, so a ctx value can be searched in the log.
std::string toCsvLine(const ErrorRecord& record)
{
    const auto context = fmt::format(
        "{}",
        fmt::join(record.context | std::views::transform([](const auto& entry) { return fmt::format("{}:{}", entry.first, entry.second); }), " "));
    return fmt::format(
        "{},\"{}\",\"{}\",{}\n",
        static_cast<uint64_t>(record.code),
        escapeCsv(record.message),
        escapeCsv(context),
        toUnixMilliseconds(record.timestamp));
}

/// Keys are uppercase because the JSON formatter uppercases unquoted field names.
std::string toJsonLine(const ErrorRecord& record)
{
    const auto context = record.context
        | std::views::transform([](const auto& entry)
                                { return fmt::format(R"("{}":"{}")", escapeJson(toUpperCase(entry.first)), escapeJson(entry.second)); });
    return fmt::format(
        R"({{"ERROR_CODE":{},"MESSAGE":"{}","CTX":{{{}}},"TS":{}}})"
        "\n",
        static_cast<uint64_t>(record.code),
        escapeJson(record.message),
        fmt::join(context, ","),
        toUnixMilliseconds(record.timestamp));
}

/// The lines assume the formatters' default delimiters. The keys mirror ConfigParametersCSVInputFormatIndexer and
/// ConfigParametersSIMDJSON, which live in the formatter modules and are not visible here.
void checkDelimiters(const std::string_view inputFormat, const InputFormatterDescriptor& formatter)
{
    if (const auto tupleDelimiter = formatter.tryGetFromConfig<char>("TUPLE_DELIMITER"); tupleDelimiter.value_or('\n') != '\n')
    {
        throw CannotOpenSource("Error source writes one error per line and needs the tuple delimiter '\\n'");
    }
    if (inputFormat != "CSV")
    {
        return;
    }
    if (const auto fieldDelimiter = formatter.tryGetFromConfig<char>("FIELD_DELIMITER"); fieldDelimiter.value_or(',') != ',')
    {
        throw CannotOpenSource("Error source writes CSV with the field delimiter ','");
    }
    if (not formatter.tryGetFromConfig<bool>("ALLOW_COMMAS_IN_STRINGS").value_or(true))
    {
        throw CannotOpenSource("Error source writes messages that contain commas and needs ALLOW_COMMAS_IN_STRINGS");
    }
}
}

ErrorSource::ErrorSource(const SourceDescriptor& sourceDescriptor)
    : startPosition(sourceDescriptor.getFromConfig(ConfigParametersError::START_POSITION))
    , maxRecords(sourceDescriptor.getFromConfig(ConfigParametersError::MAX_RECORDS))
{
    const auto inputFormat = toUpperCase(sourceDescriptor.getInputFormatType());
    if (inputFormat == "CSV")
    {
        serializeLine = toCsvLine;
    }
    else if (inputFormat == "JSON")
    {
        serializeLine = toJsonLine;
    }
    else
    {
        throw CannotOpenSource("Error source supports the input formats CSV and JSON, got {}", inputFormat);
    }
    checkDelimiters(inputFormat, sourceDescriptor.getInputFormatterDescriptor());
}

void ErrorSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    log = ErrorLog::forThisThread();
    if (!log)
    {
        throw CannotOpenSource(
            "Error source: worker {} has no error log; start it with error_log.enabled=true", Thread::getThisWorkerNodeId());
    }
    position = (startPosition == ErrorStartPosition::EARLIEST) ? log->oldestPosition() : log->latestPosition();
}

void ErrorSource::close()
{
    if (numberOfMissedRecords > 0 || numberOfOversizedRecords > 0)
    {
        NES_WARNING(
            "Error source missed {} errors that were evicted before it read them and dropped {} errors larger than a buffer",
            numberOfMissedRecords,
            numberOfOversizedRecords);
    }
    log.reset();
}

Source::FillTupleBufferResult ErrorSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    const auto buffer = tupleBuffer.getAvailableMemoryArea<char>();
    size_t offset = 0;
    /// Checked before reading: at the limit, a read would block for an error that is never emitted.
    while (maxRecords == 0 || numberOfEmittedRecords < maxRecords)
    {
        if (!pendingLine)
        {
            /// Only the first error is waited for; afterwards return what is there instead of holding back errors.
            auto [record, missed] = offset == 0 ? log->read(position, stopToken) : log->tryRead(position);
            numberOfMissedRecords += missed;
            if (!record)
            {
                if (offset > 0 || stopToken.stop_requested())
                {
                    break;
                }
                continue;
            }
            pendingLine = serializeLine(*record);
        }

        if (pendingLine->size() > buffer.size())
        {
            ++numberOfOversizedRecords;
            pendingLine.reset();
            continue;
        }
        if (pendingLine->size() > buffer.size() - offset)
        {
            break;
        }
        std::ranges::copy(*pendingLine, buffer.begin() + static_cast<std::ptrdiff_t>(offset));
        offset += pendingLine->size();
        ++numberOfEmittedRecords;
        pendingLine.reset();
    }

    if (offset == 0)
    {
        return FillTupleBufferResult::eos();
    }
    return FillTupleBufferResult::withBytes(offset);
}

DescriptorConfig::Config ErrorSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersError>(std::move(config), NAME);
}

std::ostream& ErrorSource::toString(std::ostream& str) const
{
    return str << fmt::format(
               "\nErrorSource(startPosition: {}, maxRecords: {}, emitted: {})",
               magic_enum::enum_name(startPosition),
               maxRecords,
               numberOfEmittedRecords);
}

}
