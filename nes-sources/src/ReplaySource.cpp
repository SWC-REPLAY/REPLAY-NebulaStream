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

#include <Sources/ReplaySource.hpp>

#include <cstdint>
#include <expected>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Variant.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <StoreRegistry.hpp>

namespace NES
{

namespace
{

/// NOLINTBEGIN(bugprone-throwing-static-initialization)
const ConfigField<std::string> STORE_NAME{
    Identifier::parse("STORE_NAME"),
    "Name of the store to read from",
    [](const ConfigLiteral& literal) { return NES::tryGetOr<std::string>(literal, expectedType<std::string>()); }};
/// NOLINTEND(bugprone-throwing-static-initialization)

}

Schema<QualifiedErasedConfigField, Ordered> ReplaySource::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("REPLAY_SOURCE"), STORE_NAME);
}

std::expected<ReplaySourceConfig, Exception> ReplaySourceConfig::fromConfig(const InstantiatedConfig& config)
{
    return ReplaySourceConfig{.storeName = config.get(STORE_NAME)};
}

ReplaySource::ReplaySource(const ReplaySourceConfig& config, const SourceDescriptor& sourceDescriptor)
    : storeName(config.storeName), schema(sourceDescriptor.getSchema())
{
}

void ReplaySource::open(std::shared_ptr<AbstractBufferProvider>)
{
    store = StoreManager::StoreRegistry::instance().getStore(storeName);
    if (not store.has_value())
    {
        throw InvalidConfigParameter("ReplaySource: no store named '{}' is registered", storeName);
    }
}

void ReplaySource::close()
{
    store.reset();
}

Source::FillTupleBufferResult ReplaySource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    PRECONDITION(store.has_value(), "Replay source must be opened before reading");
    if (not store->hasMore())
    {
        return FillTupleBufferResult::eos();
    }
    /// SourceThread stores this value as the tuple count, because a NATIVE source has no bytes to format.
    const uint64_t tuplesWritten = store->read(tupleBuffer, schema);
    if (tuplesWritten == 0)
    {
        return FillTupleBufferResult::eos();
    }
    return FillTupleBufferResult::withBytes(tuplesWritten);
}

std::ostream& ReplaySource::toString(std::ostream& str) const
{
    return str << fmt::format("ReplaySource(store: {})", storeName);
}

}
