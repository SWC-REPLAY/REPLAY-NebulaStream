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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorLog.hpp>

namespace NES
{

enum class ErrorStartPosition : uint8_t
{
    /// Replay what the worker's error log retained, then continue with new errors.
    EARLIEST,
    /// Only errors recorded after the query opened the source.
    LATEST,
};

/// A column of an error record; the logical source declares any of them, in any order, by these names.
enum class ErrorField : uint8_t
{
    ERROR_CODE, /// UINT64
    MESSAGE, /// VARSIZED
    CTX, /// VARSIZED, the LogContext of the throwing thread as `key:value key:value`
    TS, /// UINT64, unix milliseconds of the throw
};

/// Reads the error log of the worker it runs on (discussion #153); the worker needs `error_log.enabled=true`.
/// Writes NATIVE row-layout tuples of the logical source's schema, so the physical source needs the input format NATIVE;
/// the textual format (CSV, JSON) is chosen by the sink.
class ErrorSource final : public Source
{
public:
    static constexpr std::string_view NAME = "Error";

    explicit ErrorSource(const SourceDescriptor& sourceDescriptor);
    ~ErrorSource() override = default;

    ErrorSource(const ErrorSource&) = delete;
    ErrorSource& operator=(const ErrorSource&) = delete;
    ErrorSource(ErrorSource&&) = delete;
    ErrorSource& operator=(ErrorSource&&) = delete;

    /// Blocks until at least one error is available, then writes as many tuples as fit without waiting for more.
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Looks up the error log of this worker and positions the reader at START_POSITION.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    struct Column
    {
        ErrorField field;
        /// Offset in the tuple, including the leading null byte of a nullable field.
        size_t offset;
        bool nullable;
    };

    void writeTuple(TupleBuffer& tupleBuffer, size_t tupleIndex, const ErrorRecord& record) const;

    /// In the order of the logical source's schema, which is the row layout the NATIVE scan reads.
    std::vector<Column> columns;
    size_t tupleSize = 0;
    ErrorStartPosition startPosition;
    /// 0 = unlimited
    uint64_t maxRecords;
    std::shared_ptr<ErrorLog> log;
    /// Provides the child buffers that hold MESSAGE and CTX.
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    /// Sequence number of the next record to read from the log.
    uint64_t position = 0;
    uint64_t numberOfEmittedRecords = 0;
    uint64_t numberOfMissedRecords = 0;
};

/// NOLINTBEGIN(cert-err58-cpp,bugprone-throwing-static-initialization)
struct ConfigParametersError
{
    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, ErrorStartPosition> START_POSITION{
        "START_POSITION",
        EnumWrapper{ErrorStartPosition::EARLIEST},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(START_POSITION, config); }};

    /// Ends the stream after this many errors; lets a bounded query (e.g. a systest) finish.
    static inline const DescriptorConfig::ConfigParameter<uint64_t> MAX_RECORDS{
        "MAX_RECORDS",
        uint64_t{0},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_RECORDS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, START_POSITION, MAX_RECORDS);
};

/// NOLINTEND(cert-err58-cpp,bugprone-throwing-static-initialization)

}
