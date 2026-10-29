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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <ranges>
#include <span>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <ErrorLog.hpp>
#include <Thread.hpp>

namespace NES
{

namespace
{
DataType::Type typeOf(const ErrorField field)
{
    switch (field)
    {
        case ErrorField::ERROR_CODE:
        case ErrorField::TS:
            return DataType::Type::UINT64;
        case ErrorField::MESSAGE:
        case ErrorField::CTX:
            return DataType::Type::VARSIZED;
    }
    std::unreachable();
}

uint64_t toUnixMilliseconds(const std::chrono::system_clock::time_point timestamp)
{
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(timestamp.time_since_epoch()).count());
}

/// The `key:value key:value` form of the `[%&]` log pattern field, so a ctx value can be searched in the log.
std::string formatContext(const ErrorRecord& record)
{
    return fmt::format(
        "{}",
        fmt::join(record.context | std::views::transform([](const auto& entry) { return fmt::format("{}:{}", entry.first, entry.second); }), " "));
}

template <typename T>
void writeValue(const std::span<std::byte> slot, const T& value)
{
    std::memcpy(slot.data(), &value, sizeof(T));
}
}

ErrorSource::ErrorSource(const SourceDescriptor& sourceDescriptor)
    : maxRecords(sourceDescriptor.getFromConfig(ConfigParametersError::MAX_RECORDS))
{
    if (const auto inputFormat = toUpperCase(sourceDescriptor.getInputFormatType()); inputFormat != "NATIVE")
    {
        throw CannotOpenSource(
            "Error source writes NATIVE tuples and needs 'NATIVE' AS INPUT_FORMATTER.\"TYPE\", got {}; choose CSV or JSON at the sink",
            inputFormat);
    }
    for (const auto& field : *sourceDescriptor.getLogicalSource().getSchema())
    {
        const auto name = field.getFullyQualifiedName().begin()->asCanonicalString();
        const auto& dataType = field.getDataType();
        const auto errorField = magic_enum::enum_cast<ErrorField>(name);
        if (not errorField.has_value() or dataType.type != typeOf(*errorField))
        {
            throw CannotOpenSource(
                "Error source has no field {} of type {}; its fields are ERROR_CODE UINT64, MESSAGE VARSIZED, CTX VARSIZED, TS UINT64",
                name,
                dataType);
        }
        columns.push_back({.field = *errorField, .offset = tupleSize, .nullable = dataType.nullable});
        tupleSize += dataType.getSizeInBytesWithNull();
    }

    /// The source is constructed while its query is started on the request thread, which belongs to the worker.
    const auto log = ErrorLog::forThisThread();
    if (!log)
    {
        throw CannotOpenSource(
            "Error source: worker {} does not collect errors; start it with enable_error_source=true", Thread::getThisWorkerNodeId());
    }
    subscription = log->subscribe();
}

void ErrorSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
}

void ErrorSource::close()
{
    subscription.reset();
    bufferProvider.reset();
}

void ErrorSource::writeTuple(TupleBuffer& tupleBuffer, const size_t tupleIndex, const ErrorRecord& record) const
{
    const auto tuple = tupleBuffer.getAvailableMemoryArea().subspan(tupleIndex * tupleSize, tupleSize);
    for (const auto& [field, offset, nullable] : columns)
    {
        auto slot = tuple.subspan(offset);
        if (nullable)
        {
            /// The row layout keeps a null flag before the value; error fields are never null.
            slot[0] = std::byte{0};
            slot = slot.subspan(1);
        }
        switch (field)
        {
            case ErrorField::ERROR_CODE:
                writeValue(slot, static_cast<uint64_t>(record.code));
                break;
            case ErrorField::TS:
                writeValue(slot, toUnixMilliseconds(record.timestamp));
                break;
            case ErrorField::MESSAGE:
                writeValue(slot, TupleBufferRef::writeVarSized(tupleBuffer, *bufferProvider, std::as_bytes(std::span{record.message})));
                break;
            case ErrorField::CTX: {
                const auto context = formatContext(record);
                writeValue(slot, TupleBufferRef::writeVarSized(tupleBuffer, *bufferProvider, std::as_bytes(std::span{context})));
                break;
            }
        }
    }
}

Source::FillTupleBufferResult ErrorSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    const size_t capacity = tupleBuffer.getBufferSize() / tupleSize;
    size_t numberOfTuples = 0;
    /// Checked before reading: at the limit, a read would block for an error that is never emitted.
    while (numberOfTuples < capacity && (maxRecords == 0 || numberOfEmittedRecords < maxRecords))
    {
        /// Only the first error is waited for; afterwards return what is there instead of holding back errors.
        const auto record = numberOfTuples == 0 ? subscription->read(stopToken) : subscription->tryRead();
        if (!record)
        {
            if (numberOfTuples > 0 || stopToken.stop_requested())
            {
                break;
            }
            continue;
        }
        writeTuple(tupleBuffer, numberOfTuples, *record);
        ++numberOfTuples;
        ++numberOfEmittedRecords;
    }

    if (numberOfTuples == 0)
    {
        return FillTupleBufferResult::eos();
    }
    /// A NATIVE source reports tuples, not bytes; SourceThread stores the value as the buffer's number of tuples (cf. NetworkSource).
    return FillTupleBufferResult::withBytes(numberOfTuples);
}

DescriptorConfig::Config ErrorSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersError>(std::move(config), NAME);
}

std::ostream& ErrorSource::toString(std::ostream& str) const
{
    return str << fmt::format("\nErrorSource(maxRecords: {}, emitted: {})", maxRecords, numberOfEmittedRecords);
}

}
