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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Util/PlanRenderer.hpp>
#include "Traits/TraitSet.hpp"
#include "SerializableOperator.pb.h"
#include "Configurations/Enums/EnumWrapper.hpp"

namespace NES
{

/// Logical operator that persists rows to a binary file while passing them downstream unchanged.
class ReplayStoreLogicalOperator
{
public:
    ReplayStoreLogicalOperator() = default;

    explicit ReplayStoreLogicalOperator(DescriptorConfig::Config validatedConfig) : config(std::move(validatedConfig)) { }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] ReplayStoreLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] ReplayStoreLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] bool operator==(const ReplayStoreLogicalOperator& rhs) const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;
    [[nodiscard]] ReplayStoreLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

    void serialize(SerializableOperator& serializableOperator) const;

    [[nodiscard]] const DescriptorConfig::Config& getConfig() const { return config; }

    [[nodiscard]] ReplayStoreLogicalOperator withConfig(DescriptorConfig::Config validatedConfig) const;

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
            "file_path",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& cfg) { return DescriptorConfig::tryGet(FILE_PATH, cfg); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(FILE_PATH);
    };

    static DescriptorConfig::Config validateAndFormatConfig(std::unordered_map<std::string, std::string> configPairs);

private:
    static constexpr std::string_view NAME = "ReplayStore";
    std::vector<LogicalOperator> children;
    TraitSet traitSet;

    DescriptorConfig::Config config;
};
}
