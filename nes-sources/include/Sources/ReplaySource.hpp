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

#include <expected>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <Store.hpp>

namespace NES
{

struct ReplaySourceConfig
{
    std::string storeName;

    static std::expected<ReplaySourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Reads back the rows that a query wrote into a named store.
class ReplaySource final : public Source
{
public:
    static constexpr std::string_view NAME = "Replay";

    ReplaySource(const ReplaySourceConfig& config, const SourceDescriptor& sourceDescriptor);
    ~ReplaySource() override = default;

    ReplaySource(const ReplaySource&) = delete;
    ReplaySource& operator=(const ReplaySource&) = delete;
    ReplaySource(ReplaySource&&) = delete;
    ReplaySource& operator=(ReplaySource&&) = delete;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// The store writes finished tuples, so the buffers need no input formatting.
    [[nodiscard]] bool addsMetadata() const override { return false; }

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string storeName;
    std::optional<StoreManager::Store> store;
    StoreSchema schema;
};

}
