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
#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Pointers.hpp>
#include <Store.hpp>
#include <TimeRange.hpp>

namespace NES
{
class ReplayStoreReader;
class StoreRegistry;

/// Reads rows from a Store or a binary file, delegating I/O as appropriate.
class ReplaySource final : public Source
{
public:
    static constexpr std::string_view NAME = "Replay";
    /// `storeRegistry` is borrowed from the worker this source runs on and must outlive it.
    ReplaySource(const SourceDescriptor& sourceDescriptor, OptionalRef<StoreRegistry> storeRegistry);
    ~ReplaySource() override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    [[nodiscard]] uint32_t getRowWidthBytes() const;

    /// Read the binary store file header and reconstruct the Schema from the embedded schema text.
    static Schema readSchemaFromFile(const std::string& filePath);

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    std::string filePath;
    std::string storeName;
    /// The registry of the worker this source runs on, borrowed; the named store is resolved from it when the source
    /// opens.
    OptionalRef<StoreRegistry> storeRegistry;
    std::optional<Store> store;
    std::unique_ptr<ReplayStoreReader> reader;
    Schema schema;
    TimeRange timeRange;
    bool readComplete{false};
};

}
