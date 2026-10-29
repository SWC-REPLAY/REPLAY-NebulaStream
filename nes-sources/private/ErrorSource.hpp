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

#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
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

/// Reads the error log of the worker it runs on (discussion #153); the worker needs `error_log.enabled=true`.
/// Writes one line per error in the input format of the physical source: CSV (error_code, message, ctx, ts by position)
/// or JSON (keys ERROR_CODE, MESSAGE, CTX, TS; CTX is an object with the LogContext keys, e.g. CTX/TASK).
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

    /// Blocks until at least one error is available, then copies as many whole lines as fit without waiting for more.
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Looks up the error log of this worker and positions the reader at START_POSITION.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// toCsvLine or toJsonLine, chosen from the input format of the physical source.
    std::string (*serializeLine)(const ErrorRecord&) = nullptr;
    ErrorStartPosition startPosition;
    /// 0 = unlimited
    uint64_t maxRecords;
    std::shared_ptr<ErrorLog> log;
    /// Sequence number of the next record to read from the log.
    uint64_t position = 0;
    uint64_t numberOfEmittedRecords = 0;
    uint64_t numberOfMissedRecords = 0;
    uint64_t numberOfOversizedRecords = 0;
    /// A serialized error that did not fit into the previous buffer; lines are never split across buffers.
    std::optional<std::string> pendingLine;
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
